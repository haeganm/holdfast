package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/haeganm/holdfast/internal/replay"
)

func TestSensorFlag(t *testing.T) {
	var s sensors
	if err := s.Set("a=39.0,-77.0,60"); err != nil {
		t.Fatal(err)
	}
	if s[0].id != "a" || s[0].circle != (replay.Circle{Lat: 39, Lon: -77, RadiusKm: 60}) {
		t.Fatalf("parsed %+v", s[0])
	}
	for _, bad := range []string{"a=39.0,-77.0,60", "=39,-77,60", "b", "b=91,0,1"} {
		if err := s.Set(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

// A broken line anywhere but the end is not a killed recording: finish must
// not drop it and the rest, and must keep the partial file.
func TestFinishRefusesADamagedRecording(t *testing.T) {
	out := filepath.Join(t.TempDir(), "rec.jsonl.gz")
	partial := `{"t_ms":1000,"icao":"A1B2C3","lat":39,"lon":-77,"alt":0,"gs":0,"hdg":0,"node":"a"}
garbage
{"t_ms":2000,"icao":"A1B2C3","lat":39,"lon":-77,"alt":0,"gs":0,"hdg":0,"node":"b"}
`
	if err := os.WriteFile(out+".partial", []byte(partial), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finish(out); err == nil {
		t.Fatal("finish accepted a recording with a broken line in the middle")
	}
	if _, err := os.Stat(out + ".partial"); err != nil {
		t.Fatal("the damaged partial file was not kept")
	}
}

// A recording killed mid-write leaves an unsorted partial file whose last
// line may be cut short. finish keeps every whole line, sorts them, writes a
// trace that reads back, and removes the partial file.
func TestFinishSalvagesAKilledRecording(t *testing.T) {
	out := filepath.Join(t.TempDir(), "rec.jsonl.gz")
	partial := `{"t_ms":2000,"icao":"A1B2C3","lat":39,"lon":-77,"alt":0,"gs":0,"hdg":0,"node":"b","delay_ms":300}
{"t_ms":1000,"icao":"A1B2C3","lat":39,"lon":-77,"alt":0,"gs":0,"hdg":0,"node":"a","delay_ms":500}
{"t_ms":1000,"icao":"0000FF","lat":39,"lon":-77,"alt":0,"gs":0,"hdg":0,"node":"a"}
{"t_ms":3000,"icao":"A1B2`
	if err := os.WriteFile(out+".partial", []byte(partial), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finish(out); err != nil {
		t.Fatal(err)
	}
	recs, err := replay.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range recs {
		got = append(got, r.Node+r.ICAO)
	}
	if !slices.Equal(got, []string{"a0000FF", "aA1B2C3", "bA1B2C3"}) {
		t.Fatalf("records in order %v", got)
	}
	if _, err := os.Stat(out + ".partial"); !os.IsNotExist(err) {
		t.Fatal("the partial file is still there")
	}
}
