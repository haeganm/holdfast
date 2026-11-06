// Package replay generates, reads and plays back aircraft traces.
//
// A trace is JSON Lines, one position report per line, sorted by time, and
// optionally gzipped:
//
//	{"t_ms":1000,"icao":"A1B2C3","lat":39.1,"lon":-77.2,"alt":31000,"gs":420,"hdg":87}
//
// Two kinds exist. Synthetic traces from Generate give exact ground truth:
// every node in range receives every report at t_ms, the moment it was
// made. Recorded traces (cmd/gentrace -sensor) hold what each node actually
// received from a live feed, so a line also names its node and how long the
// feed took to deliver it:
//
//	{"t_ms":1000,"icao":"A1B2C3",...,"node":"a","delay_ms":420}
package replay

import (
	"bufio"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
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

	// Recorded traces only. Node is the node that received the report, and
	// DelayMS how long after the observation it arrived, so the aircraft was
	// observed at TMS - DelayMS. Synthetic traces leave both zero: every node
	// in range receives the report the moment it is made.
	Node    string `json:"node,omitempty"`
	DelayMS int64  `json:"delay_ms,omitempty"`
}

// maxTraceMS bounds a record's time, a year, far inside what time.Duration
// holds, so replay arithmetic can't overflow.
const maxTraceMS = 365 * 24 * 3600 * 1000

// MaxDelay bounds a recorded report's delivery delay: a position older than
// adsb.MaxPositionAge is dropped at the source, plus room for feed lag.
const MaxDelay = 2 * time.Minute

// ObservedMS is when the aircraft was observed, in ms after the trace start.
func (r Record) ObservedMS() int64 { return r.TMS - r.DelayMS }

// For reports whether the node with this id receives r: a synthetic report
// goes to every node, a recorded one only to the node that recorded it.
func (r Record) For(id string) bool { return r.Node == "" || r.Node == id }

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

var (
	icaoRe = regexp.MustCompile(`^[0-9A-F]{6}$`)
	nodeRe = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
)

// ValidNode reports whether id can name the node of a recorded report.
func ValidNode(id string) bool { return nodeRe.MatchString(id) }

// Open reads a trace file, gunzipping it if the name ends in .gz.
func Open(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		defer gz.Close()
		r = gz
	}
	recs, err := Read(r)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return recs, nil
}

// Window returns the records with from <= t_ms < from+length, shifted so the
// window starts at 0. length 0 means to the end.
func Window(recs []Record, from, length int64) []Record {
	var out []Record
	for _, r := range recs {
		if r.TMS >= from && (length == 0 || r.TMS < from+length) {
			r.TMS -= from
			out = append(out, r)
		}
	}
	return out
}

// Read parses and validates a trace. Lines must be sorted by t_ms.
func Read(r io.Reader) ([]Record, error) {
	var recs []Record
	sc := bufio.NewScanner(r)
	for line := 1; sc.Scan(); line++ {
		text := sc.Text()
		if len(strings.TrimSpace(text)) == 0 {
			continue
		}
		var rec Record
		dec := json.NewDecoder(strings.NewReader(text))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rec); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if strings.TrimSpace(text[dec.InputOffset():]) != "" {
			return nil, fmt.Errorf("line %d: more after the record", line)
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
	if r.TMS < 0 || r.TMS > maxTraceMS {
		return fmt.Errorf("t_ms %d: want 0 to %d", r.TMS, int64(maxTraceMS))
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
	if r.Node != "" && !ValidNode(r.Node) {
		return fmt.Errorf("node %q: want 1-32 of [a-z0-9-]", r.Node)
	}
	if r.DelayMS < 0 || r.DelayMS > MaxDelay.Milliseconds() {
		return fmt.Errorf("delay_ms %d: want 0 to %d", r.DelayMS, MaxDelay.Milliseconds())
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
	// Each check passes only a good value, so a NaN fails it.
	ok := c.Lat >= -90 && c.Lat <= 90 && c.Lon >= -180 && c.Lon <= 180 && c.RadiusKm > 0 && !math.IsInf(c.RadiusKm, 1)
	if !ok {
		return Circle{}, fmt.Errorf("coverage %q: out of range", s)
	}
	return c, nil
}

// Contains reports whether a position is within the circle, by great-circle
// distance on a spherical Earth.
func (c Circle) Contains(lat, lon float64) bool {
	return DistanceKm(c.Lat, c.Lon, lat, lon) <= c.RadiusKm
}

// Enclosing returns a circle around the given circles' centers' mean that
// contains every one of them: one feed query that covers several nodes.
func Enclosing(cs []Circle) Circle {
	var out Circle
	for _, c := range cs {
		out.Lat += c.Lat / float64(len(cs))
		out.Lon += c.Lon / float64(len(cs))
	}
	for _, c := range cs {
		out.RadiusKm = max(out.RadiusKm, DistanceKm(out.Lat, out.Lon, c.Lat, c.Lon)+c.RadiusKm)
	}
	return out
}

// DistanceKm is the great-circle distance on a spherical Earth.
func DistanceKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthKm = 6371.0088 // mean radius
	rad := math.Pi / 180
	dLat, dLon := (lat2-lat1)*rad, (lon2-lon1)*rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthKm * math.Asin(math.Sqrt(a))
}

// Play calls emit for each record at start plus its t_ms, in order, until the
// records run out or ctx is done. Records already due are emitted at once,
// unless ctx is done.
func Play(ctx context.Context, recs []Record, start time.Time, emit func(Record)) error {
	for _, r := range recs {
		if err := ctx.Err(); err != nil {
			return err
		}
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
