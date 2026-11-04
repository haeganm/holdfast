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

// Put merges t into the register for id. It reports whether t is now the
// stored value, and returns the value it replaced or lost to (the zero Track
// if the slot was empty). Redelivering the stored value is not a win.
func (m *Map) Put(id ICAO, t Track) (won bool, prev Track) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.m[id]
	if ok && Compare(cur, t) >= 0 {
		return false, cur
	}
	m.m[id] = t
	return true, cur
}

// MergeAll puts every track and returns how many won.
func (m *Map) MergeAll(tracks map[ICAO]Track) (won int) {
	for id, t := range tracks {
		if ok, _ := m.Put(id, t); ok {
			won++
		}
	}
	return won
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

// Hash returns a hex SHA-256 of the replica's contents. Two replicas have
// the same hash exactly when they hold equal tracks for the same aircraft.
func (m *Map) Hash() string { return HashOf(m.Snapshot()) }

// HashOf hashes a snapshot, so a caller can report a snapshot and its hash
// consistently.
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
