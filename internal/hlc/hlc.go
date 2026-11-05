// Package hlc implements a hybrid logical clock (Kulkarni et al., "Logical
// Physical Clocks", 2014).
//
// A Timestamp pairs a physical wall time in milliseconds with a logical
// counter. Compared with reading the wall clock directly, the clock adds two
// guarantees:
//
//   - Now never goes backwards, even when the physical clock does.
//   - After Update(m), every later timestamp sorts after m.
//
// The second one is causality: a node with a slow physical clock still stamps
// its next write after anything it has already received, so its fresh data is
// not silently overwritten by older data from a node whose clock runs fast.
//
// The logical counter only grows while the physical clock trails the latest
// timestamp seen. It never exceeds MaxLogical: past that the clock moves to
// the next millisecond instead, so a node that received a stamp at the bound
// never emits a logical part its peers would reject. Callers must reject
// remote stamps whose logical part is already over MaxLogical.
//
// One edge remains, and it heals itself: a stamp at both bounds at once
// (wall part exactly at a receiver's drift limit, logical part at MaxLogical)
// rolls the receiver 1 ms past that limit, and a peer whose limit is the same
// refuses its messages until its own clock moves on by that millisecond.
package hlc

import (
	"cmp"
	"fmt"
	"sync"
	"time"
)

// MaxLogical is the largest logical part a Clock produces or a node accepts.
// Honest stamps stay far below it: the counter only grows while a physical
// clock trails the latest stamp, and the node bounds that gap to its max drift.
const MaxLogical = 1 << 20

// Timestamp is a point in hybrid logical time. The zero value sorts before
// every timestamp a Clock produces.
type Timestamp struct {
	WallMS  int64  `json:"w"`
	Logical uint32 `json:"l"`
}

// Compare returns -1, 0 or +1 as t sorts before, equal to or after u.
func (t Timestamp) Compare(u Timestamp) int {
	if c := cmp.Compare(t.WallMS, u.WallMS); c != 0 {
		return c
	}
	return cmp.Compare(t.Logical, u.Logical)
}

func (t Timestamp) String() string { return fmt.Sprintf("%d.%d", t.WallMS, t.Logical) }

// Clock is safe for concurrent use.
type Clock struct {
	mu   sync.Mutex
	now  func() time.Time
	wall bool
	last Timestamp
}

// New returns a clock driven by the physical clock now. With wall set, the
// clock is the baseline the experiments compare against: it returns the
// physical time with a zero logical part and ignores remote timestamps, which
// is plain wall-clock last-writer-wins.
func New(now func() time.Time, wall bool) *Clock {
	return &Clock{now: now, wall: wall}
}

// Now returns the timestamp for a local event, such as stamping a write or a
// message about to be sent.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := c.now().UnixMilli()
	if c.wall {
		return Timestamp{WallMS: pt}
	}
	if pt > c.last.WallMS {
		c.last = Timestamp{WallMS: pt}
	} else {
		c.last.Logical++
	}
	c.rollOver()
	return c.last
}

// rollOver moves to the next millisecond rather than let the logical part
// pass MaxLogical. The result still sorts after every earlier timestamp.
func (c *Clock) rollOver() {
	if c.last.Logical > MaxLogical {
		c.last = Timestamp{WallMS: c.last.WallMS + 1}
	}
}

// Update folds in a timestamp received from another node and returns the
// timestamp of the receive event, which sorts after both m and every
// timestamp this clock produced before.
func (c *Clock) Update(m Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := c.now().UnixMilli()
	if c.wall {
		return Timestamp{WallMS: pt}
	}
	switch {
	case pt > c.last.WallMS && pt > m.WallMS:
		c.last = Timestamp{WallMS: pt}
	case c.last.WallMS == m.WallMS:
		c.last.Logical = max(c.last.Logical, m.Logical) + 1
	case c.last.WallMS > m.WallMS:
		c.last.Logical++
	default: // m is ahead of both the physical clock and anything seen so far
		c.last = Timestamp{WallMS: m.WallMS, Logical: m.Logical + 1}
	}
	c.rollOver()
	return c.last
}

// Peek returns the latest timestamp without advancing the clock. For the wall
// baseline it is the current physical time.
func (c *Clock) Peek() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.wall {
		return Timestamp{WallMS: c.now().UnixMilli()}
	}
	return c.last
}
