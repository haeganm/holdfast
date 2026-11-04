package replay

import (
	"bytes"
	"context"
	"math"
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Read(strings.NewReader(tc.input))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestCircle(t *testing.T) {
	c, err := ParseCircle("39, -77, 100")
	if err != nil {
		t.Fatal(err)
	}
	// One degree of latitude is about 111.2 km.
	if d := haversineKm(39, -77, 40, -77); math.Abs(d-111.2) > 0.2 {
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
