package adsb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/haeganm/holdfast/internal/crdt"
)

// A hand-built response in the adsb.lol shape, with every awkward case the
// live feed showed. No recorded data: the fixture's values are made up.
const apiResponse = `{"now":1700000000000,"total":11,"ctime":1700000000000,"ptime":0,"msg":"No error","ac":[
 {"hex":"a1b2c3","type":"adsb_icao","lat":39.1,"lon":-77.2,"alt_baro":31000,"gs":420.5,"track":87.2,"seen_pos":0.4},
 {"hex":"A1B2C4","lat":38.9,"lon":-77.0,"alt_baro":"ground","gs":3.1,"track":12,"seen_pos":20},
 {"hex":"a1b2c5","lat":38.8,"lon":-76.9,"seen_pos":1.5},
 {"hex":"a1b2c6","lat":38.7,"lon":-76.8,"alt_baro":12000,"gs":-1,"track":360,"seen_pos":0},
 {"hex":"~2a0b01","type":"tisb_other","lat":39.0,"lon":-77.0,"alt_baro":2000,"seen_pos":0.1},
 {"hex":"a1b2c7","alt_baro":35000,"seen":0.2},
 {"hex":"a1b2c8","lat":39.0,"lon":-77.0,"seen_pos":75},
 {"hex":"a1b2c9","lat":91,"lon":-77.0,"seen_pos":1},
 {"hex":"a1b2ca","lat":"39.0","lon":-77.0,"seen_pos":1},
 {"hex":"a1b2cb","lat":39.0,"lon":-77.0},
 {"hex":"a1b2c3","lat":39.05,"lon":-77.25,"seen_pos":3.0}
]}`

func TestParseKeepsValidAircraftAndSkipsTheRest(t *testing.T) {
	reports, skipped, made, err := Parse(strings.NewReader(apiResponse))
	if err != nil {
		t.Fatal(err)
	}
	if made.UnixMilli() != 1_700_000_000_000 {
		t.Fatalf("made at %d", made.UnixMilli())
	}
	// Skipped: non-ICAO, no position, position too old, latitude out of
	// range, latitude of the wrong type, no position age.
	if skipped != 6 {
		t.Errorf("skipped %d, want 6", skipped)
	}
	want := []Report{
		// Listed twice: the fresher position (0.4 s old, not 3 s) wins.
		{"A1B2C3", crdt.Observation{Lat: 39.1, Lon: -77.2, AltFt: 31000, GSKt: 420.5, HeadingDeg: 87.2, ObservedAtMS: 1_699_999_999_600}},
		{"A1B2C4", crdt.Observation{Lat: 38.9, Lon: -77.0, AltFt: 0, GSKt: 3.1, HeadingDeg: 12, ObservedAtMS: 1_699_999_980_000}},
		{"A1B2C5", crdt.Observation{Lat: 38.8, Lon: -76.9, ObservedAtMS: 1_699_999_998_500}},
		// Negative speed and a heading of 360 are not valid, so they read as unknown.
		{"A1B2C6", crdt.Observation{Lat: 38.7, Lon: -76.8, AltFt: 12000, ObservedAtMS: 1_700_000_000_000}},
	}
	if len(reports) != len(want) {
		t.Fatalf("%d reports, want %d: %+v", len(reports), len(want), reports)
	}
	for i := range want {
		if reports[i] != want[i] {
			t.Errorf("report %d:\n got %+v\nwant %+v", i, reports[i], want[i])
		}
	}
}

func TestParseReadsAReceiversAircraftJSON(t *testing.T) {
	// readsb's aircraft.json: "aircraft" rather than "ac", and "now" in seconds.
	body := `{"now":1700000000.5,"messages":123,"aircraft":[{"hex":"abcdef","lat":39,"lon":-77,"seen_pos":0.5}]}`
	reports, _, made, err := Parse(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if made.UnixMilli() != 1_700_000_000_500 || len(reports) != 1 || reports[0].Obs.ObservedAtMS != 1_700_000_000_000 {
		t.Fatalf("made %d, reports %+v", made.UnixMilli(), reports)
	}
}

func TestParseRefusesAResponseWithoutAClock(t *testing.T) {
	for _, body := range []string{`{"ac":[]}`, `{"now":0,"ac":[]}`, `{"now":"soon"}`, `not json`, ``,
		`{"now":1e19,"ac":[]}`, `{"now":-5,"ac":[]}`, `{"now":5000000000000,"ac":[]}`} {
		if _, _, _, err := Parse(strings.NewReader(body)); err == nil {
			t.Errorf("accepted %q", body)
		}
	}
}

func FuzzParse(f *testing.F) {
	f.Add(apiResponse)
	f.Add(`{"now":1700000000.5,"aircraft":[{"hex":"abcdef","lat":39,"lon":-77,"seen_pos":0.5}]}`)
	f.Fuzz(func(t *testing.T, body string) {
		reports, _, made, err := Parse(strings.NewReader(body))
		if err != nil {
			return
		}
		seen := map[crdt.ICAO]bool{}
		for _, r := range reports {
			o := r.Obs
			switch {
			case !hexRe.MatchString(string(r.ICAO)) || strings.ToUpper(string(r.ICAO)) != string(r.ICAO):
				t.Fatalf("bad address %q", r.ICAO)
			case seen[r.ICAO]:
				t.Fatalf("%s reported twice", r.ICAO)
			case !finite(o.Lat) || !finite(o.Lon) || !finite(o.AltFt) || !finite(o.GSKt) || !finite(o.HeadingDeg):
				t.Fatalf("non-finite number in %+v", o)
			case o.Lat < -90 || o.Lat > 90 || o.Lon < -180 || o.Lon > 180:
				t.Fatalf("position out of range in %+v", o)
			case o.ObservedAtMS > made.UnixMilli()+1 || o.ObservedAtMS < made.Add(-MaxPositionAge).UnixMilli()-1:
				t.Fatalf("observed at %d, outside the response's window ending %d", o.ObservedAtMS, made.UnixMilli())
			case o.HeadingDeg < 0 || o.HeadingDeg >= 360 || o.GSKt < 0:
				t.Fatalf("heading or speed out of range in %+v", o)
			}
			seen[r.ICAO] = true
		}
	})
}

// scripted serves the given bodies in order (a status code instead of a body
// for entries starting with a digit), then cancels the follower.
type scripted struct {
	mu     sync.Mutex
	bodies []string
	agents []string
	cancel context.CancelFunc
}

func (s *scripted) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agents = append(s.agents, r.UserAgent())
	if len(s.bodies) == 0 {
		s.cancel()
		http.Error(w, "done", http.StatusGone)
		return
	}
	b := s.bodies[0]
	s.bodies = s.bodies[1:]
	var code int
	if _, err := fmt.Sscanf(b, "%d", &code); err == nil {
		http.Error(w, "scripted", code)
		return
	}
	fmt.Fprint(w, b)
}

func follow(t *testing.T, bodies ...string) (emitted []Report, failures []error, agents []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := &scripted{bodies: bodies, cancel: cancel}
	srv := httptest.NewServer(s)
	defer srv.Close()
	f := Feed{URL: srv.URL, Every: time.Millisecond, MaxSkew: 30 * time.Second, Client: srv.Client(),
		Now: func() time.Time { return time.UnixMilli(1_700_000_000_000) }, Agent: "holdfast-test"}
	sensor := NewSensor()
	f.Follow(ctx, func(rs []Report) { emitted = append(emitted, sensor.New(rs)...) },
		func(err error) { failures = append(failures, err) })
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("the follower never finished the script")
	}
	return emitted, failures, s.agents
}

func one(nowMS int64, lat float64, seenPos float64) string {
	return fmt.Sprintf(`{"now":%d,"ac":[{"hex":"abcdef","lat":%v,"lon":-77,"seen_pos":%v}]}`, nowMS, lat, seenPos)
}

func TestFollowEmitsEachObservationOnce(t *testing.T) {
	const now = 1_700_000_000_000
	emitted, failures, agents := follow(t,
		one(now, 39.0, 0.5),       // new aircraft
		one(now+1000, 39.0, 1.5),  // same message seen again: same observation time
		one(now+1300, 39.0, 1.6),  // observed 0.2 s after the last one, same position: a rounding-level repeat
		one(now+2000, 39.01, 0.5), // it moved
		one(now+5000, 39.01, 0.5), // parked, but heard again 3 s later
	)
	if len(failures) != 0 { // the response that ends the script comes after the cancel, so it isn't one
		t.Fatalf("failures: %v", failures)
	}
	var at []int64
	for _, r := range emitted {
		at = append(at, r.Obs.ObservedAtMS)
	}
	if fmt.Sprint(at) != fmt.Sprint([]int64{now - 500, now + 1500, now + 4500}) {
		t.Fatalf("emitted observations at %v", at)
	}
	if agents[0] != "holdfast-test" {
		t.Fatalf("user agent %q", agents[0])
	}
}

func TestFollowReportsAndSurvivesABadFeed(t *testing.T) {
	const now = 1_700_000_000_000
	emitted, failures, _ := follow(t,
		"429",
		"503",
		`{"now":`,                    // cut off
		one(now-5*60_000, 39.0, 0.5), // the feed's clock is 5 minutes off ours
		one(now, 39.0, 0.5),
	)
	var busy *busyError
	if len(failures) != 4 || !errors.As(failures[0], &busy) || !errors.As(failures[1], &busy) {
		t.Fatalf("failures: %v", failures)
	}
	if !strings.Contains(failures[3].Error(), "clock") {
		t.Fatalf("a skewed feed was not refused for its clock: %v", failures[3])
	}
	if len(emitted) != 1 {
		t.Fatalf("emitted %d reports after the feed recovered, want 1", len(emitted))
	}
}

func TestBackoffNeverUndercutsTheIntervalOrRetryAfter(t *testing.T) {
	for _, c := range []struct{ last, every, retry, want time.Duration }{
		{3 * time.Second, 3 * time.Second, 0, 6 * time.Second},
		{24 * time.Second, 3 * time.Second, 0, maxBackoff},
		{45 * time.Second, 45 * time.Second, 0, 45 * time.Second}, // a slow feed's own interval stands
		{45 * time.Second, 45 * time.Second, 2 * time.Minute, 2 * time.Minute},
		{3 * time.Second, 3 * time.Second, 10 * time.Second, 10 * time.Second},
	} {
		if got := backoff(c.last, c.every, c.retry); got != c.want {
			t.Errorf("backoff(%v, %v, %v) = %v, want %v", c.last, c.every, c.retry, got, c.want)
		}
	}
}

func TestFollowRefusesANonPositiveInterval(t *testing.T) {
	var failed error
	Feed{URL: "http://unused", Every: 0}.Follow(context.Background(), func([]Report) { t.Fatal("polled") },
		func(err error) { failed = err })
	if failed == nil {
		t.Fatal("an interval of 0 would poll a public feed in a tight loop")
	}
}

func TestSensorForgetsAircraftLongGoneQuiet(t *testing.T) {
	s := NewSensor()
	s.New([]Report{{"AAAAA1", crdt.Observation{Lat: 1, ObservedAtMS: 1}}})
	s.New([]Report{{"AAAAA2", crdt.Observation{Lat: 1, ObservedAtMS: 1 + forget.Milliseconds() + 1}}})
	if _, kept := s.last["AAAAA1"]; kept || len(s.last) != 1 {
		t.Fatalf("still remembers %d aircraft, including one quiet for over %v", len(s.last), forget)
	}
}

func TestParseKeepsTheSameDuplicateWhateverTheOrder(t *testing.T) {
	one := `{"hex":"abcdef","lat":39,"lon":-77,"alt_baro":1000,"seen_pos":1}`
	two := `{"hex":"abcdef","lat":39,"lon":-77,"alt_baro":2000,"seen_pos":1}`
	a, _, _, _ := Parse(strings.NewReader(`{"now":1700000000000,"ac":[` + one + "," + two + "]}"))
	b, _, _, _ := Parse(strings.NewReader(`{"now":1700000000000,"ac":[` + two + "," + one + "]}"))
	if len(a) != 1 || len(b) != 1 || a[0] != b[0] {
		t.Fatalf("order changed the result: %+v vs %+v", a, b)
	}
}

func TestPollReadsRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	_, err := Feed{URL: srv.URL, Client: srv.Client(), Now: time.Now}.Poll(context.Background())
	var busy *busyError
	if !errors.As(err, &busy) || busy.retryAfter != 7*time.Second {
		t.Fatalf("err %v, want a refusal asking for 7 s", err)
	}
}
