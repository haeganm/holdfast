package crdt

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"testing/quick"

	"github.com/haeganm/holdfast/internal/hlc"
)

// genTrack draws from deliberately tiny domains so that equal stamps, equal
// origins and stamp ties with different contents all happen often. Those are
// the cases a merge rule gets wrong.
func genTrack(r *rand.Rand) Track {
	small := []float64{0, math.Copysign(0, -1), 1, 2}
	return Track{
		Obs: Observation{
			Lat:          small[r.IntN(len(small))],
			Lon:          small[r.IntN(len(small))],
			AltFt:        float64(r.IntN(2)),
			GSKt:         float64(r.IntN(2)),
			HeadingDeg:   float64(r.IntN(2)),
			ObservedAtMS: int64(r.IntN(3)),
		},
		TS:     hlc.Timestamp{WallMS: int64(r.IntN(4)), Logical: uint32(r.IntN(2))},
		Origin: []string{"a", "b", "c"}[r.IntN(3)],
	}
}

func rng(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, 0xd1b54a32d192ed03)) }

func TestCompareIsATotalOrder(t *testing.T) {
	prop := func(seed uint64) bool {
		r := rng(seed)
		a, b, c := genTrack(r), genTrack(r), genTrack(r)
		if Compare(a, b) != -Compare(b, a) {
			t.Logf("not antisymmetric: %+v vs %+v", a, b)
			return false
		}
		if (Compare(a, b) == 0) != (a == b) {
			t.Logf("Compare==0 must mean equal values: %+v vs %+v", a, b)
			return false
		}
		if Compare(a, b) <= 0 && Compare(b, c) <= 0 && Compare(a, c) > 0 {
			t.Logf("not transitive: %+v %+v %+v", a, b, c)
			return false
		}
		return true
	}
	if err := quick.Check(prop, propertyConfig(t)); err != nil {
		t.Fatal(err)
	}
}

// Expiry has to commute with merging, or two replicas that expire at the
// same cutoff could still end up different, and expiry would need tombstones.
func TestExpireCommutesWithMerge(t *testing.T) {
	prop := func(seed uint64) bool {
		r := rng(seed)
		cutoff := int64(r.IntN(5))
		a, b := NewMap(), NewMap()
		for range 8 {
			a.Put(ICAO(fmt.Sprintf("A%05d", r.IntN(4))), genTrack(r))
			b.Put(ICAO(fmt.Sprintf("A%05d", r.IntN(4))), genTrack(r))
		}
		merged := NewMap()
		merged.MergeAll(a.Snapshot())
		merged.MergeAll(b.Snapshot())
		merged.Expire(cutoff)
		a.Expire(cutoff)
		b.Expire(cutoff)
		separately := NewMap()
		separately.MergeAll(a.Snapshot())
		separately.MergeAll(b.Snapshot())
		for _, t := range separately.Snapshot() {
			if t.TS.WallMS < cutoff {
				return false
			}
		}
		return merged.Hash() == separately.Hash()
	}
	if err := quick.Check(prop, propertyConfig(t)); err != nil {
		t.Fatal(err)
	}
}

// Ord must order exactly as Compare does, over the cases a string encoding
// gets wrong: origins that are prefixes of each other, negative numbers,
// -0 against +0, and ties on everything but one field.
func TestOrdOrdersLikeCompare(t *testing.T) {
	origins := []string{"a", "a-", "a0", "ab", "b", "z", "0", "-"}
	nums := []float64{math.Inf(-1), -1e9, -1.5, -0.25, math.Copysign(0, -1), 0, 0.25, 1.5, 1e9, math.Inf(1)}
	prop := func(seed uint64) bool {
		r := rng(seed)
		num := func() float64 { return nums[r.IntN(len(nums))] }
		a := genTrack(r)
		a.Origin = origins[r.IntN(len(origins))]
		a.Obs.Lat, a.Obs.Lon, a.Obs.AltFt, a.Obs.GSKt, a.Obs.HeadingDeg = num(), num(), num(), num(), num()
		a.Obs.ObservedAtMS = int64(r.IntN(5)) - 2
		// b is a with exactly one field changed, so every level of the
		// tie-break gets compared, not just the stamp.
		b := a
		switch r.IntN(8) {
		case 0:
			b.TS.WallMS = int64(r.IntN(4))
		case 1:
			b.TS.Logical = uint32(r.IntN(2))
		case 2:
			b.Origin = origins[r.IntN(len(origins))]
		case 3:
			b.Obs.ObservedAtMS = int64(r.IntN(5)) - 2
		case 4:
			b.Obs.Lat = num()
		case 5:
			b.Obs.Lon = num()
		case 6:
			b.Obs.AltFt, b.Obs.GSKt = num(), num()
		case 7:
			b.Obs.HeadingDeg = num()
		}
		return cmp.Compare(Ord(a), Ord(b)) == Compare(a, b)
	}
	if err := quick.Check(prop, propertyConfig(t)); err != nil {
		t.Fatal(err)
	}
}

func TestMergeIsCommutativeAssociativeIdempotent(t *testing.T) {
	prop := func(seed uint64) bool {
		r := rng(seed)
		a, b, c := genTrack(r), genTrack(r), genTrack(r)
		return Merge(a, b) == Merge(b, a) &&
			Merge(Merge(a, b), c) == Merge(a, Merge(b, c)) &&
			Merge(a, a) == a
	}
	if err := quick.Check(prop, propertyConfig(t)); err != nil {
		t.Fatal(err)
	}
}

func TestPutKeepsTheGreaterTrack(t *testing.T) {
	m := NewMap()
	older := Track{TS: hlc.Timestamp{WallMS: 1}, Origin: "a", Obs: Observation{ObservedAtMS: 5}}
	newer := Track{TS: hlc.Timestamp{WallMS: 2}, Origin: "a", Obs: Observation{ObservedAtMS: 9}}

	if won, _ := m.Put("A00001", older); !won {
		t.Fatal("Put into an empty slot must win")
	}
	if won, prev := m.Put("A00001", newer); !won || prev != older {
		t.Fatalf("newer Put: won=%v prev=%+v, want won=true prev=older", won, prev)
	}
	if won, prev := m.Put("A00001", older); won || prev != newer {
		t.Fatalf("older Put: won=%v prev=%+v, want won=false prev=newer", won, prev)
	}
	if won, _ := m.Put("A00001", newer); won {
		t.Fatal("re-delivering the stored value must not count as a win")
	}
}

// exchange runs one round of the anti-entropy protocol between two replicas,
// exactly as the node does over HTTP: a sends its digest, b answers with what
// a is missing and names what it wants back, a sends that.
func exchange(a, b *Map) {
	newer, want := b.Diff(a.Digest())
	a.MergeAll(newer)
	b.MergeAll(a.Get(want))
}

// Convergence: however updates are split across replicas and whatever order
// pairwise exchanges happen in, once every replica is connected to every
// other through some chain of exchanges, all replicas hold the same map, and
// that map is the per-key maximum of every update ever made.
func TestReplicasConvergeThroughDigestExchange(t *testing.T) {
	prop := func(seed uint64) bool {
		r := rng(seed)
		const replicas, keys, updates = 5, 20, 200
		reps := make([]*Map, replicas)
		for i := range reps {
			reps[i] = NewMap()
		}
		truth := NewMap()
		for range updates {
			id := ICAO(fmt.Sprintf("A%05X", r.IntN(keys)))
			tr := genTrack(r)
			reps[r.IntN(replicas)].Put(id, tr)
			truth.Put(id, tr)
			if r.IntN(4) == 0 { // exchanges interleave with writes
				i, j := r.IntN(replicas), r.IntN(replicas)
				if i != j {
					exchange(reps[i], reps[j])
					if reps[i].Hash() != reps[j].Hash() {
						t.Logf("seed %d: replicas %d and %d differ right after exchanging", seed, i, j)
						return false
					}
				}
			}
		}
		// A chain forward then back connects every pair.
		for i := 0; i+1 < replicas; i++ {
			exchange(reps[i], reps[i+1])
		}
		for i := replicas - 1; i > 0; i-- {
			exchange(reps[i], reps[i-1])
		}
		want := truth.Snapshot()
		for i, rep := range reps {
			if got := rep.Snapshot(); !maps.Equal(got, want) {
				t.Logf("seed %d: replica %d holds %d keys, truth %d, or values differ", seed, i, len(got), len(want))
				return false
			}
			if rep.Hash() != truth.Hash() {
				t.Logf("seed %d: replica %d hash differs from truth", seed, i)
				return false
			}
		}
		return true
	}
	if err := quick.Check(prop, propertyConfig(t)); err != nil {
		t.Fatal(err)
	}
}

// A stamp tie between different contents can only happen under the wall-clock
// baseline, but if the digest compared stamps alone the two replicas would
// each believe they were up to date and never converge.
func TestStampTieWithDifferentContentsStillConverges(t *testing.T) {
	stamp := hlc.Timestamp{WallMS: 7}
	x := Track{TS: stamp, Origin: "a", Obs: Observation{Lat: 1, ObservedAtMS: 1}}
	y := Track{TS: stamp, Origin: "a", Obs: Observation{Lat: 2, ObservedAtMS: 1}}
	a, b := NewMap(), NewMap()
	a.Put("A00001", x)
	b.Put("A00001", y)
	exchange(a, b)
	if a.Hash() != b.Hash() {
		t.Fatalf("replicas differ after exchange: a=%+v b=%+v", a.Snapshot(), b.Snapshot())
	}
	if got := a.Snapshot()["A00001"]; got != Merge(x, y) {
		t.Fatalf("kept %+v, want the merge winner %+v", got, Merge(x, y))
	}
}

func TestNegativeZeroHashesLikeZero(t *testing.T) {
	a, b := NewMap(), NewMap()
	a.Put("A00001", Track{Origin: "a", Obs: Observation{Lat: 0}})
	b.Put("A00001", Track{Origin: "a", Obs: Observation{Lat: math.Copysign(0, -1)}})
	if a.Hash() != b.Hash() {
		t.Fatal("0 and -0 compare equal, so they must hash equal or replicas never look converged")
	}
}

func TestHashIgnoresInsertionOrder(t *testing.T) {
	r := rng(1)
	tracks := map[ICAO]Track{}
	for i := range 50 {
		tracks[ICAO(fmt.Sprintf("B%05X", i))] = genTrack(r)
	}
	a, b := NewMap(), NewMap()
	a.MergeAll(tracks)
	for _, id := range []ICAO{"B00031", "B00002", "B00017"} {
		b.Put(id, tracks[id])
	}
	b.MergeAll(tracks)
	if a.Hash() != b.Hash() {
		t.Fatal("same contents, different insertion order, different hash")
	}
	if a.Hash() == NewMap().Hash() {
		t.Fatal("hash of a non-empty map equals the empty map's")
	}
}

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
