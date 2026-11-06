// Package crdt holds the fused aircraft picture: a map from aircraft to a
// last-writer-wins register, and the digest exchange that makes replicas
// converge.
//
// Merge picks the greater of two tracks under Compare, a total order over
// track values. Taking a maximum under a total order is commutative,
// associative and idempotent, so the map is a state-based CRDT: replicas that
// have seen the same set of writes hold the same map, whatever order the
// writes arrived in and however often they were redelivered.
package crdt

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math"
	"slices"
	"sync"

	"github.com/haeganm/holdfast/internal/hlc"
)

// ICAO is the 24-bit aircraft address as six uppercase hex digits.
type ICAO string

// Observation is one position report. ObservedAtMS is when the aircraft was
// observed, in true time; it never takes part in conflict resolution except
// as a last tie-break, and exists so experiments can tell whether the winner
// of a merge was actually the fresher report.
type Observation struct {
	Lat          float64 `json:"lat"`
	Lon          float64 `json:"lon"`
	AltFt        float64 `json:"alt"`
	GSKt         float64 `json:"gs"`
	HeadingDeg   float64 `json:"hdg"`
	ObservedAtMS int64   `json:"at"`
}

// Track is the register value for one aircraft: the observation, the hybrid
// timestamp it was written with, and the node that wrote it.
type Track struct {
	Obs    Observation   `json:"obs"`
	TS     hlc.Timestamp `json:"ts"`
	Origin string        `json:"origin"`
}

// Compare orders tracks by stamp, then origin, then contents. Under the HLC a
// node never issues the same stamp twice, so (stamp, origin) already decides.
// The wall-clock baseline can issue a stamp twice, and the content tie-break
// keeps the order total there too; without it Merge would depend on argument
// order and replicas could disagree forever.
func Compare(a, b Track) int {
	if c := a.TS.Compare(b.TS); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Origin, b.Origin); c != 0 {
		return c
	}
	return cmp.Or(
		cmp.Compare(a.Obs.ObservedAtMS, b.Obs.ObservedAtMS),
		cmp.Compare(a.Obs.Lat, b.Obs.Lat),
		cmp.Compare(a.Obs.Lon, b.Obs.Lon),
		cmp.Compare(a.Obs.AltFt, b.Obs.AltFt),
		cmp.Compare(a.Obs.GSKt, b.Obs.GSKt),
		cmp.Compare(a.Obs.HeadingDeg, b.Obs.HeadingDeg),
	)
}

// Ord encodes t as a string whose byte order is Compare's order, so a store
// that compares strings (DynamoDB condition expressions do) can enforce
// "only a greater track replaces this one" without decoding it. It covers
// every field Compare breaks ties on; a checksum would not do, since on a
// stamp tie it could order two values differently from Compare. t must be
// valid: an origin of [a-z0-9-], no NaN.
func Ord(t Track) string {
	b := make([]byte, 0, 128)
	b = fmt.Appendf(b, "%016x%08x", uint64(t.TS.WallMS)^1<<63, t.TS.Logical)
	b = append(b, t.Origin...)
	b = append(b, ' ') // below every byte an origin may hold, so "a" sorts before "a-" as it does in Go
	b = fmt.Appendf(b, "%016x", uint64(t.Obs.ObservedAtMS)^1<<63)
	for _, f := range []float64{t.Obs.Lat, t.Obs.Lon, t.Obs.AltFt, t.Obs.GSKt, t.Obs.HeadingDeg} {
		b = fmt.Appendf(b, "%016x", orderedBits(f))
	}
	return string(b)
}

// orderedBits maps a float to an unsigned integer with the same order: flip
// the sign bit of positives, every bit of negatives. -0 and +0 compare equal,
// so -0 is mapped as +0.
func orderedBits(f float64) uint64 {
	u := math.Float64bits(canon(f))
	if u>>63 == 1 {
		return ^u
	}
	return u | 1<<63
}

// Merge returns the greater track.
func Merge(a, b Track) Track {
	if Compare(a, b) >= 0 {
		return a
	}
	return b
}

// Version is what a digest carries per aircraft: enough to decide who is
// ahead without shipping the observation. Sum is a checksum of the contents,
// used only to spot a stamp tie between different values.
type Version struct {
	TS     hlc.Timestamp `json:"ts"`
	Origin string        `json:"origin"`
	Sum    uint64        `json:"sum"`
}

func versionOf(t Track) Version { return Version{TS: t.TS, Origin: t.Origin, Sum: sum(t.Obs)} }

// Map is a replica of the fused picture. It is safe for concurrent use.
type Map struct {
	mu sync.RWMutex
	m  map[ICAO]Track
}

func NewMap() *Map { return &Map{m: map[ICAO]Track{}} }

// Put merges t into the register for id: the slot ends up holding Merge of
// what it held and t. It reports whether t is now the stored value, and
// returns the value it replaced or lost to (the zero Track if the slot was
// empty). Redelivering the stored value is not a win.
func (m *Map) Put(id ICAO, t Track) (won bool, prev Track) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.m[id]
	if ok && Merge(cur, t) == cur {
		return false, cur
	}
	m.m[id] = t
	return true, cur
}

// Digest summarizes the replica for a peer.
func (m *Map) Digest() map[ICAO]Version {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d := make(map[ICAO]Version, len(m.m))
	for id, t := range m.m {
		d[id] = versionOf(t)
	}
	return d
}

// Diff compares this replica with a peer's digest. newer holds the tracks the
// peer lacks or holds an older version of; want lists the aircraft the peer is
// ahead on or that only the peer has. On a stamp tie with different contents
// both sides send their value and Merge decides, which is what makes the tie
// converge.
func (m *Map) Diff(peer map[ICAO]Version) (newer map[ICAO]Track, want []ICAO) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	newer = map[ICAO]Track{}
	for id, mine := range m.m {
		theirs, ok := peer[id]
		if !ok {
			newer[id] = mine
			continue
		}
		mv := versionOf(mine)
		c := mv.TS.Compare(theirs.TS)
		if c == 0 {
			c = cmp.Compare(mv.Origin, theirs.Origin)
		}
		switch {
		case c > 0:
			newer[id] = mine
		case c < 0:
			want = append(want, id)
		case mv.Sum != theirs.Sum:
			newer[id] = mine
			want = append(want, id)
		}
	}
	for id := range peer {
		if _, ok := m.m[id]; !ok {
			want = append(want, id)
		}
	}
	slices.Sort(want)
	return newer, want
}

// Expire drops every track stamped before cutoffMS (wall time) and returns
// the aircraft it dropped, sorted. Whether a track has expired depends only
// on its stamp and the cutoff, so expiring commutes with merging: replicas
// that expire at the same cutoff hold the same tracks, with no tombstones.
func (m *Map) Expire(cutoffMS int64) []ICAO {
	m.mu.Lock()
	defer m.mu.Unlock()
	var gone []ICAO
	for id, t := range m.m {
		if t.TS.WallMS < cutoffMS {
			delete(m.m, id)
			gone = append(gone, id)
		}
	}
	slices.Sort(gone)
	return gone
}

// Get returns the stored tracks for the given aircraft, skipping any this
// replica does not hold.
func (m *Map) Get(ids []ICAO) map[ICAO]Track {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[ICAO]Track, len(ids))
	for _, id := range ids {
		if t, ok := m.m[id]; ok {
			out[id] = t
		}
	}
	return out
}

// Snapshot returns a copy of the replica.
func (m *Map) Snapshot() map[ICAO]Track {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[ICAO]Track, len(m.m))
	for id, t := range m.m {
		out[id] = t
	}
	return out
}

func (m *Map) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.m)
}

// Has reports whether the replica holds a track for id.
func (m *Map) Has(id ICAO) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.m[id]
	return ok
}

// HashOf returns a hex SHA-256 of a snapshot's contents. Two snapshots have
// the same hash exactly when they hold equal tracks for the same aircraft.
func HashOf(tracks map[ICAO]Track) string {
	ids := make([]ICAO, 0, len(tracks))
	for id := range tracks {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	h := sha256.New()
	var buf []byte
	for _, id := range ids {
		v := versionOf(tracks[id])
		buf = buf[:0]
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(id)))
		buf = append(buf, id...)
		buf = binary.BigEndian.AppendUint64(buf, uint64(v.TS.WallMS))
		buf = binary.BigEndian.AppendUint32(buf, v.TS.Logical)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(v.Origin)))
		buf = append(buf, v.Origin...)
		buf = binary.BigEndian.AppendUint64(buf, v.Sum)
		h.Write(buf)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// sum is a 64-bit FNV-1a checksum of an observation's contents.
func sum(o Observation) uint64 {
	h := fnv.New64a()
	var buf [48]byte
	b := buf[:0]
	for _, f := range []float64{o.Lat, o.Lon, o.AltFt, o.GSKt, o.HeadingDeg} {
		b = binary.BigEndian.AppendUint64(b, math.Float64bits(canon(f)))
	}
	b = binary.BigEndian.AppendUint64(b, uint64(o.ObservedAtMS))
	h.Write(b)
	return h.Sum64()
}

// canon maps -0 to +0: they compare equal, so they must checksum equal.
func canon(f float64) float64 {
	if f == 0 {
		return 0
	}
	return f
}
