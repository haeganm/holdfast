package hlc

import (
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"testing/quick"
	"time"
)

// fakeTime is a settable physical clock in milliseconds.
type fakeTime struct{ ms int64 }

func (f *fakeTime) now() time.Time { return time.UnixMilli(f.ms) }

func ts(w int64, l uint32) Timestamp { return Timestamp{WallMS: w, Logical: l} }

func TestNowFollowsPhysicalTime(t *testing.T) {
	f := &fakeTime{ms: 1000}
	c := New(f.now, false)
	if got := c.Now(); got != ts(1000, 0) {
		t.Fatalf("Now() = %v, want 1000.0", got)
	}
	f.ms = 1005
	if got := c.Now(); got != ts(1005, 0) {
		t.Fatalf("Now() = %v, want 1005.0", got)
	}
}

func TestNowInSameMillisecondBumpsLogical(t *testing.T) {
	f := &fakeTime{ms: 1000}
	c := New(f.now, false)
	c.Now()
	if got := c.Now(); got != ts(1000, 1) {
		t.Fatalf("second Now() in the same ms = %v, want 1000.1", got)
	}
}

func TestNowNeverGoesBackwards(t *testing.T) {
	f := &fakeTime{ms: 1000}
	c := New(f.now, false)
	first := c.Now()
	f.ms = 900 // the physical clock steps back, as after an NTP correction
	second := c.Now()
	if second.Compare(first) <= 0 {
		t.Fatalf("Now went backwards: %v then %v", first, second)
	}
	if second != ts(1000, 1) {
		t.Fatalf("Now() after a backwards step = %v, want 1000.1", second)
	}
}

func TestUpdateCases(t *testing.T) {
	cases := []struct {
		name   string
		phys   int64     // physical clock at Update time
		last   Timestamp // clock state before Update (set by a Now at last.WallMS)
		remote Timestamp
		want   Timestamp
	}{
		{"physical clock ahead of both", 2000, ts(1000, 0), ts(1500, 7), ts(2000, 0)},
		{"remote ahead", 1000, ts(1000, 0), ts(1500, 7), ts(1500, 8)},
		{"local ahead", 1000, ts(1000, 0), ts(800, 9), ts(1000, 1)},
		{"same wall, remote logical higher", 1000, ts(1000, 0), ts(1000, 5), ts(1000, 6)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeTime{ms: tc.last.WallMS}
			c := New(f.now, false)
			if got := c.Now(); got != tc.last {
				t.Fatalf("setup: Now() = %v, want %v", got, tc.last)
			}
			f.ms = tc.phys
			got := c.Update(tc.remote)
			if got != tc.want {
				t.Fatalf("Update(%v) = %v, want %v", tc.remote, got, tc.want)
			}
			if got.Compare(tc.remote) <= 0 || got.Compare(tc.last) <= 0 {
				t.Fatalf("Update(%v) = %v does not dominate both inputs", tc.remote, got)
			}
		})
	}
}

// The property the whole measurement rests on: a node whose physical clock runs
// 500 ms behind a peer still stamps its next write after a write it has received.
// A plain wall clock gets this wrong.
func TestSlowClockOrdersAfterWhatItHasSeen(t *testing.T) {
	peer := New((&fakeTime{ms: 10_500}).now, false)
	remote := peer.Now()

	slowPhys := &fakeTime{ms: 10_000}

	slow := New(slowPhys.now, false)
	slow.Update(remote)
	if local := slow.Now(); local.Compare(remote) <= 0 {
		t.Fatalf("HLC: local write %v sorts before the remote write %v it has seen", local, remote)
	}

	wall := New(slowPhys.now, true)
	wall.Update(remote)
	if local := wall.Now(); local.Compare(remote) >= 0 {
		t.Fatalf("wall baseline: local write %v should sort before remote %v; the baseline is not reproducing the bug it exists to show", local, remote)
	}
}

// A remote stamp at the logical bound, or a burst of local events in one
// millisecond, must never push the clock's logical part past MaxLogical:
// peers reject such stamps, so the node would cut itself off. The clock
// moves to the next millisecond instead, which still sorts after everything.
func TestLogicalPartNeverPassesTheBound(t *testing.T) {
	f := &fakeTime{ms: 1000}
	c := New(f.now, false)
	remote := ts(1000, MaxLogical)
	got := c.Update(remote)
	if got != ts(1001, 0) || got.Compare(remote) <= 0 {
		t.Fatalf("Update(%v) = %v, want 1001.0", remote, got)
	}

	c = New(f.now, false)
	c.Update(ts(1000, MaxLogical-1))
	prev := c.Peek()
	for range 3 {
		next := c.Now()
		if next.Logical > MaxLogical || next.Compare(prev) <= 0 {
			t.Fatalf("Now() = %v after %v", next, prev)
		}
		prev = next
	}
	if prev.WallMS != 1001 {
		t.Fatalf("after passing the bound the clock is at %v, want it rolled to 1001", prev)
	}
}

func TestWallBaselineIsPlainPhysicalTime(t *testing.T) {
	f := &fakeTime{ms: 1000}
	c := New(f.now, true)
	if got := c.Update(ts(5000, 3)); got != ts(1000, 0) {
		t.Fatalf("wall Update = %v, want 1000.0 (remote time ignored)", got)
	}
	f.ms = 900
	if got := c.Now(); got != ts(900, 0) {
		t.Fatalf("wall Now = %v, want 900.0 (follows the physical clock, even backwards)", got)
	}
}

// Strict monotonicity under random interleavings of Now and Update, with a
// physical clock that jitters both ways and remote stamps up to a second off.
func TestMonotonicUnderRandomInterleavings(t *testing.T) {
	prop := func(seed uint64) bool {
		r := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
		f := &fakeTime{ms: 1_000_000}
		c := New(f.now, false)
		var last Timestamp
		for range 500 {
			f.ms += r.Int64N(21) - 10
			var got Timestamp
			if r.IntN(2) == 0 {
				got = c.Now()
			} else {
				remote := ts(f.ms+r.Int64N(2001)-1000, uint32(r.IntN(4)))
				got = c.Update(remote)
				if got.Compare(remote) <= 0 {
					t.Logf("seed %d: Update(%v) = %v", seed, remote, got)
					return false
				}
			}
			if got.Compare(last) <= 0 {
				t.Logf("seed %d: %v after %v", seed, got, last)
				return false
			}
			if got.Compare(c.Peek()) != 0 {
				t.Logf("seed %d: Peek %v != last returned %v", seed, c.Peek(), got)
				return false
			}
			last = got
		}
		return true
	}
	if err := quick.Check(prop, propertyConfig(t)); err != nil {
		t.Fatal(err)
	}
}

// propertyConfig reads HOLDFAST_PROPERTY_RUNS so check.sh can run more cases
// than a plain `go test`.
func propertyConfig(t *testing.T) *quick.Config {
	t.Helper()
	n := 200
	if s := os.Getenv("HOLDFAST_PROPERTY_RUNS"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 1 {
			t.Fatalf("HOLDFAST_PROPERTY_RUNS=%q: want a positive integer", s)
		}
		n = v
	}
	return &quick.Config{MaxCount: n}
}
