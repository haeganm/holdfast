// Package replay generates, reads and plays back aircraft traces.
//
// A trace is JSON Lines, one position report per line, sorted by time:
//
//	{"t_ms":1000,"icao":"A1B2C3","lat":39.1,"lon":-77.2,"alt":31000,"gs":420,"hdg":87}
//
// The experiments use synthetic traces from Generate rather than recorded
// ADS-B data: the ground truth has to be exact (which node saw what, when) to
// measure staleness, and recorded data has gaps and jitter that make "the
// freshest report" ambiguous. A recorded feed converts to the same format.
package replay

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Record is one position report, at TMS milliseconds after the trace start.
type Record struct {
	TMS        int64   `json:"t_ms"`
	ICAO       string  `json:"icao"`
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	AltFt      float64 `json:"alt"`
	GSKt       float64 `json:"gs"`
	HeadingDeg float64 `json:"hdg"`
}

// The generated airspace: a box around northern Virginia.
const (
	boxLatMin, boxLatMax = 38.55, 39.45
	boxLonMin, boxLonMax = -78.35, -75.65
)

// Generate returns a deterministic trace: n aircraft flying straight lines at
// constant speed and altitude, each reporting once a second at its own phase
// within the second, for dur. The same seed always yields the same trace.
func Generate(seed uint64, n int, dur time.Duration) []Record {
	r := rand.New(rand.NewPCG(seed, 0x853c49e6748fea9b))
	seen := map[string]bool{}
	var recs []Record
	for range n {
		var id string
		for id == "" || seen[id] {
			id = fmt.Sprintf("%06X", r.IntN(1<<24))
		}
		seen[id] = true
		lat0 := boxLatMin + r.Float64()*(boxLatMax-boxLatMin)
		lon0 := boxLonMin + r.Float64()*(boxLonMax-boxLonMin)
		gs := float64(250 + r.IntN(231))     // knots
		hdg := float64(r.IntN(360))          // degrees true
		alt := float64(80+r.IntN(301)) * 100 // feet
		phase := int64(r.IntN(1000))         // ms into each second
		for t := phase; t < dur.Milliseconds(); t += 1000 {
			nm := gs * float64(t) / 3_600_000 // distance flown so far
			rad := hdg * math.Pi / 180
			lat := lat0 + nm*math.Cos(rad)/60
			lon := lon0 + nm*math.Sin(rad)/(60*math.Cos(lat0*math.Pi/180))
			recs = append(recs, Record{TMS: t, ICAO: id, Lat: round(lat, 5), Lon: round(lon, 5), AltFt: alt, GSKt: gs, HeadingDeg: hdg})
		}
	}
	slices.SortFunc(recs, func(a, b Record) int {
		return cmp.Or(cmp.Compare(a.TMS, b.TMS), strings.Compare(a.ICAO, b.ICAO))
	})
	return recs
}

func round(x float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(x*p) / p
}

// Write writes recs as JSON Lines.
func Write(w io.Writer, recs []Record) error {
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return bw.Flush()
}

var icaoRe = regexp.MustCompile(`^[0-9A-F]{6}$`)

// Read parses and validates a trace. Lines must be sorted by t_ms.
func Read(r io.Reader) ([]Record, error) {
	var recs []Record
	sc := bufio.NewScanner(r)
	for line := 1; sc.Scan(); line++ {
		if len(strings.TrimSpace(sc.Text())) == 0 {
			continue
		}
		var rec Record
		dec := json.NewDecoder(strings.NewReader(sc.Text()))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rec); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if err := check(rec); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if n := len(recs); n > 0 && rec.TMS < recs[n-1].TMS {
			return nil, fmt.Errorf("line %d: t_ms %d is before the previous line's %d; traces must be sorted", line, rec.TMS, recs[n-1].TMS)
		}
		recs = append(recs, rec)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return recs, nil
}

func check(r Record) error {
	if r.TMS < 0 {
		return fmt.Errorf("negative t_ms %d", r.TMS)
	}
	if !icaoRe.MatchString(r.ICAO) {
		return fmt.Errorf("icao %q must be six uppercase hex digits", r.ICAO)
	}
	for _, f := range []float64{r.Lat, r.Lon, r.AltFt, r.GSKt, r.HeadingDeg} {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return errors.New("non-finite number")
		}
	}
	if r.Lat < -90 || r.Lat > 90 || r.Lon < -180 || r.Lon > 180 {
		return fmt.Errorf("position %v,%v out of range", r.Lat, r.Lon)
	}
	return nil
}

// Circle is a node's coverage: the reports it would receive.
type Circle struct{ Lat, Lon, RadiusKm float64 }

// ParseCircle parses "lat,lon,radius_km".
func ParseCircle(s string) (Circle, error) {
	parts := strings.Split(s, ",")
	if len(parts) != 3 {
		return Circle{}, fmt.Errorf("coverage %q: want lat,lon,radius_km", s)
	}
	var v [3]float64
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return Circle{}, fmt.Errorf("coverage %q: %w", s, err)
		}
		v[i] = f
	}
	c := Circle{Lat: v[0], Lon: v[1], RadiusKm: v[2]}
	if c.Lat < -90 || c.Lat > 90 || c.Lon < -180 || c.Lon > 180 || !(c.RadiusKm > 0) {
		return Circle{}, fmt.Errorf("coverage %q: out of range", s)
	}
	return c, nil
}

// Contains reports whether a position is within the circle, by great-circle
// distance on a spherical Earth.
func (c Circle) Contains(lat, lon float64) bool {
	return haversineKm(c.Lat, c.Lon, lat, lon) <= c.RadiusKm
}

func haversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthKm = 6371.0088 // mean radius
	rad := math.Pi / 180
	dLat, dLon := (lat2-lat1)*rad, (lon2-lon1)*rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthKm * math.Asin(math.Sqrt(a))
}

// Play calls emit for each record at start plus its t_ms, in order, until the
// records run out or ctx is done. Records already due are emitted at once.
func Play(ctx context.Context, recs []Record, start time.Time, emit func(Record)) error {
	for _, r := range recs {
		if d := time.Until(start.Add(time.Duration(r.TMS) * time.Millisecond)); d > 0 {
			t := time.NewTimer(d)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
		emit(r)
	}
	return nil
}
