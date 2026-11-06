// Package adsb reads live aircraft positions in the readsb JSON format: the
// aircraft.json that a receiver running readsb writes, and the same shape
// the adsb.lol API serves for a point and radius. It is a sensor: it turns a
// feed into observations and nothing else.
//
// The feed is untrusted. One bad aircraft is skipped, not the whole
// response, and a response whose clock disagrees with ours is refused, since
// every observation time in it would be wrong by the same amount.
package adsb

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/haeganm/holdfast/internal/crdt"
)

// Report is one aircraft's latest position in a response.
type Report struct {
	ICAO crdt.ICAO
	Obs  crdt.Observation
}

const (
	// MaxPositionAge drops positions older than this when the response was
	// made. readsb itself stops reporting a position after 60 s.
	MaxPositionAge = 60 * time.Second
	maxBody        = 8 << 20

	// A response's clock must read between 2000 and 2100. Past that it is
	// broken or hostile, and converting it to integer milliseconds could
	// overflow.
	minNowMS = 946_684_800_000
	maxNowMS = 4_102_444_800_000
)

var hexRe = regexp.MustCompile(`^[0-9a-fA-F]{6}$`)

type response struct {
	Now      *float64          `json:"now"`
	AC       []json.RawMessage `json:"ac"`       // the adsb.lol API
	Aircraft []json.RawMessage `json:"aircraft"` // a receiver's aircraft.json
}

type aircraft struct {
	Hex     string          `json:"hex"`
	Lat     *float64        `json:"lat"`
	Lon     *float64        `json:"lon"`
	AltBaro json.RawMessage `json:"alt_baro"` // feet, or the string "ground"
	GS      *float64        `json:"gs"`
	Track   *float64        `json:"track"`
	SeenPos *float64        `json:"seen_pos"` // seconds since the position was received
}

// Parse reads one response. It returns the valid aircraft sorted by address,
// how many it skipped, and the time the response says it was made.
func Parse(r io.Reader) (reports []Report, skipped int, made time.Time, err error) {
	var resp response
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return nil, 0, time.Time{}, fmt.Errorf("decode: %w", err)
	}
	if resp.Now == nil || !finite(*resp.Now) || *resp.Now <= 0 {
		return nil, 0, time.Time{}, errors.New(`response has no usable "now"`)
	}
	// The API gives milliseconds, a receiver's aircraft.json seconds.
	nowMS := *resp.Now
	if nowMS < 1e11 {
		nowMS *= 1000
	}
	if nowMS < minNowMS || nowMS > maxNowMS {
		return nil, 0, time.Time{}, fmt.Errorf(`response clock "now" of %v is not between 2000 and 2100`, *resp.Now)
	}
	made = time.UnixMilli(int64(nowMS))
	for _, raw := range append(resp.AC, resp.Aircraft...) {
		rep, ok := parseAircraft(raw, nowMS)
		if !ok {
			skipped++
			continue
		}
		reports = append(reports, rep)
	}
	// A response lists an aircraft once. If one lists it twice, keep the
	// freshest position, ties broken on the position, so the result never
	// depends on the order of the input.
	slices.SortFunc(reports, func(a, b Report) int {
		return cmp.Or(
			strings.Compare(string(a.ICAO), string(b.ICAO)),
			cmp.Compare(b.Obs.ObservedAtMS, a.Obs.ObservedAtMS),
			cmp.Compare(a.Obs.Lat, b.Obs.Lat),
			cmp.Compare(a.Obs.Lon, b.Obs.Lon),
			cmp.Compare(a.Obs.AltFt, b.Obs.AltFt),
			cmp.Compare(a.Obs.GSKt, b.Obs.GSKt),
			cmp.Compare(a.Obs.HeadingDeg, b.Obs.HeadingDeg),
		)
	})
	reports = slices.CompactFunc(reports, func(a, b Report) bool { return a.ICAO == b.ICAO })
	return reports, skipped, made, nil
}

func parseAircraft(raw json.RawMessage, nowMS float64) (Report, bool) {
	var a aircraft
	if json.Unmarshal(raw, &a) != nil {
		return Report{}, false
	}
	// A leading ~ marks an address that is not an ICAO 24-bit address
	// (TIS-B and similar); it could collide with a real aircraft's.
	if !hexRe.MatchString(a.Hex) || a.Lat == nil || a.Lon == nil || a.SeenPos == nil {
		return Report{}, false
	}
	lat, lon, seen := *a.Lat, *a.Lon, *a.SeenPos
	if !finite(lat) || !finite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return Report{}, false
	}
	if !finite(seen) || seen < 0 || seen > MaxPositionAge.Seconds() {
		return Report{}, false
	}
	obs := crdt.Observation{Lat: lat, Lon: lon, ObservedAtMS: int64(math.Round(nowMS - seen*1000))}
	var alt float64
	if json.Unmarshal(a.AltBaro, &alt) == nil && finite(alt) {
		obs.AltFt = alt // "ground", missing or junk all leave 0
	}
	if a.GS != nil && finite(*a.GS) && *a.GS >= 0 {
		obs.GSKt = *a.GS
	}
	if a.Track != nil && finite(*a.Track) && *a.Track >= 0 && *a.Track < 360 {
		obs.HeadingDeg = *a.Track
	}
	return Report{ICAO: crdt.ICAO(strings.ToUpper(a.Hex)), Obs: obs}, true
}

func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// PointURL is the adsb.lol-style query for a circle: base/lat/lon/radius,
// the radius in whole nautical miles, rounded up so the circle is covered.
func PointURL(base string, lat, lon, radiusKm float64) string {
	return fmt.Sprintf("%s/%.4f/%.4f/%d", strings.TrimRight(base, "/"), lat, lon, int(math.Ceil(radiusKm/1.852)))
}

// Feed polls one URL that serves the readsb format.
type Feed struct {
	URL   string
	Every time.Duration
	// MaxSkew refuses a response whose own clock is further than this from
	// ours, since every observation time in it would be off by as much.
	MaxSkew time.Duration
	Client  *http.Client
	Now     func() time.Time
	Agent   string // User-Agent; public feeds ask clients to identify themselves
}

// maxBackoff caps the wait after a failed poll. Several pollers sharing one
// feed's allowance must not starve each other by backing off for minutes.
const maxBackoff = 30 * time.Second

// maxRetryAfter caps the wait a refusal's Retry-After can ask for. It is
// longer than any rate limit's window, but the feed is untrusted, and one
// header must not silence a sensor for a day.
const maxRetryAfter = 5 * time.Minute

// backoff is the wait after a failed poll: the last wait doubled, up to
// maxBackoff, but never shorter than the normal interval or than what the
// feed asked for in Retry-After.
func backoff(last, every, retryAfter time.Duration) time.Duration {
	return max(min(2*last, maxBackoff), every, retryAfter)
}

// Follow polls the feed until ctx is done, the first time at once, and calls
// got with every successful response. fail is called for every failed poll,
// with how long until the next one: after any failure the wait backs off
// (see backoff).
func (f Feed) Follow(ctx context.Context, got func([]Report), fail func(error)) {
	if f.Every <= 0 {
		fail(fmt.Errorf("poll interval %v: must be positive", f.Every))
		return
	}
	wait := f.Every
	for next := time.Now(); ; {
		reports, err := f.Poll(ctx)
		var busy *busyError
		switch {
		case err == nil:
			wait = f.Every
			got(reports)
		case ctx.Err() != nil:
			return
		case errors.As(err, &busy):
			wait = backoff(wait, f.Every, busy.retryAfter)
		default:
			wait = backoff(wait, f.Every, 0)
		}
		if err != nil {
			fail(fmt.Errorf("%w; next poll in %v", err, wait))
		}
		// A fixed start-to-start schedule, so a slow response doesn't
		// stretch the interval; a poll that overran its slot goes at once.
		next = next.Add(wait)
		if d := time.Until(next); d > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
			}
		} else if ctx.Err() != nil {
			return
		} else {
			next = time.Now()
		}
	}
}

// Sensor turns successive responses into new observations. A feed keeps
// returning an aircraft's last position until it hears the next one, so a
// report is passed on only if the position changed, or the same position was
// heard again more than a second later (an aircraft parked at a gate).
type Sensor struct {
	last map[crdt.ICAO]crdt.Observation
}

func NewSensor() *Sensor { return &Sensor{last: map[crdt.ICAO]crdt.Observation{}} }

// forget drops what a sensor remembers about an aircraft it hasn't heard
// from in this long, so a live sensor's memory stays bounded.
const forget = 10 * time.Minute

// New returns the reports that are new observations, in order.
func (s *Sensor) New(reports []Report) []Report {
	var out []Report
	var newest int64
	for _, r := range reports {
		newest = max(newest, r.Obs.ObservedAtMS)
		prev, seen := s.last[r.ICAO]
		moved := !seen || prev.Lat != r.Obs.Lat || prev.Lon != r.Obs.Lon
		if r.Obs.ObservedAtMS > prev.ObservedAtMS && (moved || r.Obs.ObservedAtMS-prev.ObservedAtMS > 1000) {
			s.last[r.ICAO] = r.Obs
			out = append(out, r)
		}
	}
	maps.DeleteFunc(s.last, func(_ crdt.ICAO, o crdt.Observation) bool { return o.ObservedAtMS < newest-forget.Milliseconds() })
	return out
}

type busyError struct {
	status     string
	retryAfter time.Duration
}

func (e *busyError) Error() string { return "feed is refusing requests: " + e.status }

// retryAfter reads a Retry-After header, in seconds or as an HTTP date, as a
// wait from now of 0 to maxRetryAfter.
func retryAfter(h string, now time.Time) time.Duration {
	if s, err := strconv.Atoi(h); err == nil {
		return time.Duration(min(max(s, 0), int(maxRetryAfter/time.Second))) * time.Second
	}
	if at, err := http.ParseTime(h); err == nil {
		return min(max(at.Sub(now), 0), maxRetryAfter)
	}
	return 0
}

// Poll makes one request.
func (f Feed) Poll(ctx context.Context) ([]Report, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", f.Agent)
	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, &busyError{resp.Status, retryAfter(resp.Header.Get("Retry-After"), f.Now())}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", f.URL, resp.Status)
	}
	if len(body) > maxBody {
		return nil, fmt.Errorf("%s: response over %d bytes", f.URL, maxBody)
	}
	reports, _, made, err := Parse(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if skew := f.Now().Sub(made); skew > f.MaxSkew || skew < -f.MaxSkew {
		return nil, fmt.Errorf("feed clock is %v off ours, over the %v allowed", skew.Round(time.Millisecond), f.MaxSkew)
	}
	return reports, nil
}
