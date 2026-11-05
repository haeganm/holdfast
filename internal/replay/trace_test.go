package replay

import (
	"bytes"
	"compress/gzip"
	"context"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func encode(t *testing.T, recs []Record) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := Write(&b, recs); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestGenerateIsDeterministic(t *testing.T) {
	a := encode(t, Generate(7, 30, 20*time.Second))
	b := encode(t, Generate(7, 30, 20*time.Second))
	if !bytes.Equal(a, b) {
		t.Fatal("same seed produced different traces")
	}
	if bytes.Equal(a, encode(t, Generate(8, 30, 20*time.Second))) {
		t.Fatal("different seeds produced the same trace")
	}
}

func TestGenerateShape(t *testing.T) {
	recs := Generate(1, 30, 20*time.Second)
	if len(recs) != 30*20 {
		t.Fatalf("%d records, want one per aircraft per second (600)", len(recs))
	}
	ids := map[string]int{}
	for i, r := range recs {
		ids[r.ICAO]++
		if i > 0 && r.TMS < recs[i-1].TMS {
			t.Fatalf("record %d out of order", i)
		}
		if r.TMS < 0 || r.TMS >= 20_000 {
			t.Fatalf("record at t_ms %d outside the trace", r.TMS)
		}
	}
	if len(ids) != 30 {
		t.Fatalf("%d distinct aircraft, want 30", len(ids))
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	recs := Generate(3, 10, 5*time.Second)
	got, err := Read(bytes.NewReader(encode(t, recs)))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, recs) {
		t.Fatal("round trip changed the records")
	}
}

func TestReadRejectsBadLines(t *testing.T) {
	ok := `{"t_ms":0,"icao":"ABCDEF","lat":39,"lon":-77,"alt":1,"gs":1,"hdg":1}`
	for _, tc := range []struct{ name, input, wantErr string }{
		{"lowercase icao", `{"t_ms":0,"icao":"abcdef","lat":39,"lon":-77,"alt":1,"gs":1,"hdg":1}`, "line 1: icao"},
		{"unsorted", `{"t_ms":5,"icao":"ABCDEF","lat":39,"lon":-77,"alt":1,"gs":1,"hdg":1}` + "\n" + ok, "line 2: t_ms 0 is before"},
		{"unknown field", `{"t_ms":0,"icao":"ABCDEF","lat":39,"lon":-77,"alt":1,"gs":1,"hdg":1,"x":1}`, "line 1: json: unknown field"},
		{"latitude", `{"t_ms":0,"icao":"ABCDEF","lat":91,"lon":-77,"alt":1,"gs":1,"hdg":1}`, "out of range"},
		{"garbage", ok + "\nnot json", "line 2:"},
		{"t_ms past a year", `{"t_ms":9300000000000,"icao":"ABCDEF","lat":39,"lon":-77,"alt":1,"gs":1,"hdg":1}`, "t_ms"},
		{"bad node", `{"t_ms":0,"icao":"ABCDEF","lat":39,"lon":-77,"alt":1,"gs":1,"hdg":1,"node":"A B"}`, "node"},
		{"negative delay", `{"t_ms":0,"icao":"ABCDEF","lat":39,"lon":-77,"alt":1,"gs":1,"hdg":1,"delay_ms":-1}`, "delay_ms"},
		{"absurd delay", `{"t_ms":0,"icao":"ABCDEF","lat":39,"lon":-77,"alt":1,"gs":1,"hdg":1,"delay_ms":3600000}`, "delay_ms"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Read(strings.NewReader(tc.input))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestRecordedFieldsRoundTripThroughAGzippedFile(t *testing.T) {
	recs := []Record{
		{TMS: 100, ICAO: "A1B2C3", Lat: 39, Lon: -77, Node: "a", DelayMS: 400},
		{TMS: 900, ICAO: "A1B2C3", Lat: 39.01, Lon: -77, Node: "b", DelayMS: 0},
		{TMS: 1500, ICAO: "D4E5F6", Lat: 38.9, Lon: -76.9},
	}
	path := filepath.Join(t.TempDir(), "t.jsonl.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	if err := Write(gz, recs); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	got, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, recs) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", got, recs)
	}
	if got[0].ObservedMS() != -300 || got[2].ObservedMS() != 1500 {
		t.Fatalf("observed at %d and %d", got[0].ObservedMS(), got[2].ObservedMS())
	}
	if !got[0].For("a") || got[0].For("b") || !got[2].For("b") {
		t.Fatal("a recorded report must go only to its own node, a synthetic one to every node")
	}
}

func TestWindowShiftsToZero(t *testing.T) {
	recs := []Record{{TMS: 500}, {TMS: 60_000}, {TMS: 119_999}, {TMS: 120_000}}
	got := Window(recs, 60_000, 60_000)
	if len(got) != 2 || got[0].TMS != 0 || got[1].TMS != 59_999 {
		t.Fatalf("window: %+v", got)
	}
	if len(Window(recs, 0, 0)) != 4 {
		t.Fatal("length 0 must mean to the end")
	}
}

func TestEnclosingCoversEveryCircle(t *testing.T) {
	cs := []Circle{{39.0, -77.0, 60}, {39.0, -76.2, 60}, {39.0, -77.8, 60}}
	e := Enclosing(cs)
	for _, c := range cs {
		// the far edge of each circle, east and west
		for _, dLon := range []float64{-0.78, 0.78} {
			if c.Contains(c.Lat, c.Lon+dLon) && !e.Contains(c.Lat, c.Lon+dLon) {
				t.Fatalf("enclosing %+v misses the edge of %+v", e, c)
			}
		}
	}
	if e.RadiusKm > 200 {
		t.Fatalf("enclosing radius %v km is far larger than needed", e.RadiusKm)
	}
}

func TestCircle(t *testing.T) {
	c, err := ParseCircle("39, -77, 100")
	if err != nil {
		t.Fatal(err)
	}
	// One degree of latitude is about 111.2 km.
	if d := DistanceKm(39, -77, 40, -77); math.Abs(d-111.2) > 0.2 {
		t.Fatalf("one degree of latitude = %.2f km, want about 111.2", d)
	}
	if !c.Contains(39.8, -77) || c.Contains(40, -77) {
		t.Fatal("Contains disagrees with the distances")
	}
	for _, bad := range []string{"39,-77", "39,-77,0", "x,-77,10", "95,-77,10"} {
		if _, err := ParseCircle(bad); err == nil {
			t.Fatalf("ParseCircle(%q) accepted bad input", bad)
		}
	}
}

func TestPlayEmitsOnScheduleAndStops(t *testing.T) {
	recs := []Record{{TMS: 0, ICAO: "AAAAAA"}, {TMS: 30, ICAO: "BBBBBB"}, {TMS: 60, ICAO: "CCCCCC"}}
	start := time.Now()
	var got []string
	var at []time.Duration
	if err := Play(context.Background(), recs, start, func(r Record) {
		got = append(got, r.ICAO)
		at = append(at, time.Since(start))
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"AAAAAA", "BBBBBB", "CCCCCC"}) {
		t.Fatalf("emitted %v", got)
	}
	if at[2] < 60*time.Millisecond {
		t.Fatalf("last record emitted after %v, before its 60 ms schedule", at[2])
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n := 0
	err := Play(ctx, []Record{{TMS: 0}, {TMS: 60_000}}, time.Now(), func(Record) { n++ })
	if err == nil || n != 1 {
		t.Fatalf("after cancel: err=%v emitted=%d, want an error after the one record already due", err, n)
	}
}
