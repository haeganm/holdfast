package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/haeganm/holdfast/internal/crdt"
	"github.com/haeganm/holdfast/internal/hlc"
)

// TestSimulation drives a small cluster through random schedules of faults:
// lost, delayed, duplicated and cut-off messages, lost responses, partitions,
// clock jumps in both directions and restarts that wipe a node. Everything
// runs in one goroutine on a fake clock and every fault is drawn from the
// seed, so a failure replays exactly:
//
//	HOLDFAST_SIM_SEED=17 go test -run TestSimulation ./internal/node
//
// After every step it checks the invariants the design rests on, and at the
// end it stops the faults, lets every pair sync and checks that every replica
// holds the merge of everything any node held.
func TestSimulation(t *testing.T) {
	seeds := simSeeds(t)
	for _, wall := range []bool{false, true} {
		cov := &simCoverage{}
		for _, seed := range seeds {
			s := newSim(t, seed, wall)
			s.seen = cov
			s.run(600)
			if t.Failed() {
				return
			}
		}
		t.Logf("wall=%v: %+v", wall, *cov)
		if len(seeds) < 100 {
			continue // a single replayed seed can't be held to totals
		}
		min := len(seeds) // at least about one of each per schedule
		if cov.cleanRounds < 20*min || cov.restarts < min || cov.expired < min {
			t.Errorf("wall=%v: schedules exercised too little: %+v", wall, *cov)
		}
		for kind, n := range cov.faults {
			if n < min {
				t.Errorf("wall=%v: fault kind %d injected only %d times", wall, kind, n)
			}
		}
		if !wall && cov.ties > 0 {
			t.Errorf("hybrid clock: %d stamps reused for different values", cov.ties)
		}
		if wall && cov.ties < min/10 {
			t.Errorf("wall clock: only %d stamp ties with different contents; the tie-break is untested", cov.ties)
		}
	}
}

func simSeeds(t *testing.T) []uint64 {
	if s := os.Getenv("HOLDFAST_SIM_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("HOLDFAST_SIM_SEED=%q: want an unsigned integer", s)
		}
		return []uint64{seed}
	}
	runs := 200
	if s := os.Getenv("HOLDFAST_SIM_RUNS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			t.Fatalf("HOLDFAST_SIM_RUNS=%q: want a positive integer", s)
		}
		runs = n
	}
	seeds := make([]uint64, runs)
	for i := range seeds {
		seeds[i] = uint64(i)
	}
	return seeds
}

const (
	simStartMS = 1_700_000_000_000 // true time when a schedule starts
	simSkewMS  = 2000              // clocks stay within this of true time, so the spread is under the max drift
	// Time moves and clocks jump in 10 ms steps, so the wall clock often
	// reissues a stamp it used before. That is the only way two different
	// values get the same stamp and origin, which the content tie-break exists
	// for.
	simTickMS = 10
	simTTL    = 10 * time.Second // short enough that tracks expire mid-schedule
)

// The airspace: a window of four aircraft slides along a pool of twelve, one
// step every 5 s, so aircraft arrive, go quiet and expire the way real
// traffic does. Four at a time keeps conflicts on one aircraft frequent.
var simAircraft = []crdt.ICAO{"A00001", "A00002", "A00003", "A00004", "A00005", "A00006",
	"A00007", "A00008", "A00009", "A0000A", "A0000B", "A0000C"}

func (s *sim) aircraft() crdt.ICAO {
	window := int((s.trueMS - simStartMS) / 5000)
	return simAircraft[(window+s.rng.IntN(4))%len(simAircraft)]
}

type sim struct {
	t      *testing.T
	seed   uint64
	wall   bool
	rng    *rand.Rand
	trueMS int64
	step   int
	nodes  []*simNode // fixed order, so the schedule never depends on map order
	byID   map[string]*simNode
	cut    map[[2]string]bool
	faulty bool
	faults int // faults injected so far; a round with none must leave both sides equal
	held   []heldRequest
	seen   *simCoverage
}

// simCoverage counts what the schedules actually exercised, across seeds.
// TestSimulation fails if any count is too low, so a change that makes the
// schedule vacuous (every round failing, no ties, no restarts) can't pass.
type simCoverage struct {
	cleanRounds, restarts, ties, expired int
	faults                               [5]int // lost, delayed, cut off, response lost, duplicated
}

type simNode struct {
	id       string
	offsetMS int64 // this node's physical clock error, changed by clock jumps
	n        *Node
	seen     map[crdt.ICAO]crdt.Track // the replica at the last check
	clock    hlc.Timestamp            // the clock at the last check
	floor    hlc.Timestamp            // the highest clock anywhere when this node last started
	gone     map[crdt.ICAO]crdt.Track // versions this node expired, which must never come back
}

// heldRequest is a request the network delayed: the sender saw a failure,
// and the receiver gets it later, possibly after newer ones.
type heldRequest struct {
	from, to, path string
	body           []byte
}

func newSim(t *testing.T, seed uint64, wall bool) *sim {
	s := &sim{
		t: t, seed: seed, wall: wall,
		rng:    rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15)),
		trueMS: simStartMS,
		byID:   map[string]*simNode{},
		cut:    map[[2]string]bool{},
		faulty: true,
	}
	for _, id := range []string{"a", "b", "c", "d"}[:3+s.rng.IntN(2)] {
		sn := &simNode{id: id, offsetMS: s.randomOffset()}
		s.nodes = append(s.nodes, sn)
		s.byID[id] = sn
	}
	for _, sn := range s.nodes {
		s.start(sn)
	}
	return s
}

// start (re)creates a node with an empty replica and a fresh clock, as a
// process restart would. floor bounds every stamp that exists at this moment,
// in any replica or still in flight, since every clock is at or above the
// stamps it issued.
func (s *sim) start(sn *simNode) {
	sn.floor = hlc.Timestamp{}
	for _, o := range s.nodes {
		if o.n != nil && o.n.clock.Peek().Compare(sn.floor) > 0 {
			sn.floor = o.n.clock.Peek()
		}
	}
	n, err := New(Config{
		ID:   sn.id,
		Wall: s.wall,
		// Now is this node's physical clock reading, error included: clock
		// jumps change offsetMS under a running node, which ClockOffset can't.
		Now:      func() time.Time { return time.UnixMilli(s.trueMS + sn.offsetMS) },
		Client:   &http.Client{Transport: simTransport{s, sn.id}},
		TrackTTL: simTTL,
	})
	if err != nil {
		s.t.Fatal(err)
	}
	sn.n, sn.seen, sn.clock, sn.gone = n, nil, hlc.Timestamp{}, map[crdt.ICAO]crdt.Track{}
}

func (s *sim) failf(format string, args ...any) {
	s.t.Helper()
	s.t.Fatalf("seed %d wall=%v step %d: %s", s.seed, s.wall, s.step, fmt.Sprintf(format, args...))
}

func (s *sim) randomOffset() int64 {
	return simTickMS * (s.rng.Int64N(2*simSkewMS/simTickMS+1) - simSkewMS/simTickMS)
}

func (s *sim) pick() *simNode { return s.nodes[s.rng.IntN(len(s.nodes))] }

func (s *sim) other(sn *simNode) *simNode {
	for {
		if o := s.pick(); o != sn {
			return o
		}
	}
}

func link(a, b string) [2]string {
	if a > b {
		a, b = b, a
	}
	return [2]string{a, b}
}

func (s *sim) run(steps int) {
	s.t.Helper()
	for s.step = 0; s.step < steps; s.step++ {
		s.trueMS += simTickMS * max(0, s.rng.Int64N(20)-5) // no time passes in about a third of steps
		switch r := s.rng.IntN(100); {
		case r < 35:
			s.ingest(s.pick())
		case r < 70:
			s.syncPair(s.pick())
		case r < 78:
			s.deliverHeld()
		case r < 86:
			a := s.pick()
			l := link(a.id, s.other(a).id)
			s.cut[l] = !s.cut[l]
		case r < 94:
			s.pick().offsetMS = s.randomOffset()
		case r < 97:
			s.start(s.pick())
			s.seen.restarts++
		default:
			s.burst(s.pick())
		}
		s.check()
	}
	s.settle()
}

// burst is a sensor delivering two reports of one aircraft in the same
// millisecond, with a sync landing between them. Under the wall clock both
// get the same stamp and origin, which is what the content tie-break is for.
func (s *sim) burst(sn *simNode) {
	id := s.aircraft()
	s.ingestAircraft(sn, id)
	s.syncPair(sn)
	s.ingestAircraft(sn, id)
}

func (s *sim) ingest(sn *simNode) {
	s.ingestAircraft(sn, s.aircraft())
}

func (s *sim) ingestAircraft(sn *simNode, id crdt.ICAO) {
	obs := crdt.Observation{
		Lat: 38 + 2*s.rng.Float64(), Lon: -78 + 2*s.rng.Float64(),
		AltFt: float64(s.rng.IntN(40_000)), GSKt: float64(s.rng.IntN(500)), HeadingDeg: float64(s.rng.IntN(360)),
		ObservedAtMS: s.trueMS,
	}
	sn.n.Ingest(id, obs)
	if s.wall || sn.n.held != nil {
		return
	}
	// The write landed after the startup hold, so it must sort after every
	// stamp that existed when this node started, its own old ones included.
	if got := sn.n.tracks.Snapshot()[id]; got.Obs != obs || got.TS.Compare(sn.floor) <= 0 {
		s.failf("node %s wrote %s at %v after its hold, not above %v, the highest clock when it started", sn.id, id, got.TS, sn.floor)
	}
}

// syncPair runs one anti-entropy round. If the network injected no fault,
// both sides must end up holding exactly the same picture.
func (s *sim) syncPair(a *simNode) {
	b := s.other(a)
	before := s.faults
	err := a.n.syncOnce(context.Background(), "http://"+b.id)
	if err == nil && s.faults == before {
		cut := max(a.n.cutoff(), b.n.cutoff())
		if live(a.n, cut) != live(b.n, cut) {
			s.failf("a clean round between %s and %s left them different", a.id, b.id)
		}
		s.seen.cleanRounds++
	}
	if err != nil && s.faults == before && !s.cut[link(a.id, b.id)] {
		s.failf("a round between %s and %s failed with no fault injected: %v", a.id, b.id, err)
	}
}

// live hashes the tracks a node holds that are stamped at or after cut.
func live(n *Node, cut int64) string {
	snap := n.tracks.Snapshot()
	maps.DeleteFunc(snap, func(_ crdt.ICAO, t crdt.Track) bool { return t.TS.WallMS < cut })
	return crdt.HashOf(snap)
}

func (s *sim) deliverHeld() {
	if len(s.held) == 0 {
		return
	}
	i := s.rng.IntN(len(s.held))
	h := s.held[i]
	if s.cut[link(h.from, h.to)] {
		return
	}
	s.held = append(s.held[:i], s.held[i+1:]...)
	s.deliver(h.to, h.path, bytes.NewReader(h.body)).Body.Close()
}

// check asserts the invariants after every step.
func (s *sim) check() {
	s.t.Helper()
	for _, sn := range s.nodes {
		snap := sn.n.tracks.Snapshot()
		clock := sn.n.clock.Peek()
		c := sn.n.Counters()
		if c.Rejected != 0 {
			s.failf("node %s rejected %d messages from honest peers", sn.id, c.Rejected)
		}
		if !s.wall {
			if clock.Compare(sn.clock) < 0 {
				s.failf("node %s clock went back from %v to %v", sn.id, sn.clock, clock)
			}
			for id, tr := range snap {
				if tr.TS.Compare(clock) > 0 {
					s.failf("node %s stores %s at %v, above its clock %v", sn.id, id, tr.TS, clock)
				}
			}
			if c.IngestLost != 0 {
				s.failf("node %s lost %d local writes under the HLC", sn.id, c.IngestLost)
			}
		}
		if sn.n.held == nil {
			for id := range sn.n.own {
				if _, ok := snap[id]; !ok {
					s.failf("node %s reported %s but its replica has nothing for it", sn.id, id)
				}
			}
		}
		for id, cur := range snap {
			if g, ok := sn.gone[id]; ok && g.TS == cur.TS && g.Origin == cur.Origin {
				s.failf("node %s holds %s at %v again after expiring it", sn.id, id, cur.TS)
			}
		}
		cut := sn.n.cutoff()
		for id, prev := range sn.seen {
			cur, ok := snap[id]
			if !ok {
				if prev.TS.WallMS >= cut {
					s.failf("node %s dropped %s, stamped %v, before it expired at %d", sn.id, id, prev.TS, cut)
				}
				sn.gone[id] = prev
				s.seen.expired++
				continue
			}
			if crdt.Compare(cur, prev) < 0 {
				s.failf("node %s moved %s down from %+v to %+v", sn.id, id, prev, cur)
			}
		}
		sn.seen, sn.clock = snap, clock
	}
}

// settle stops the faults, delivers what the network still held, then lets
// every pair sync twice. Every replica must then equal the merge of
// everything any node held when the faults stopped.
func (s *sim) settle() {
	s.t.Helper()
	s.faulty = false
	clear(s.cut)
	for _, sn := range s.nodes {
		sn.offsetMS = 0 // replicas agree on expiry only to within their clock spread
	}
	for _, h := range s.held {
		s.deliver(h.to, h.path, bytes.NewReader(h.body)).Body.Close()
	}
	s.held = nil
	// Let every startup hold run out, so the picture is final before the
	// expected merge is taken.
	s.trueMS += 2*simSkewMS + 5000
	for _, sn := range s.nodes {
		sn.n.mu.Lock()
		if sn.n.holding() {
			s.failf("node %s still holding reports after its hold should have ended", sn.id)
		}
		sn.n.mu.Unlock()
	}
	s.check()

	want := map[crdt.ICAO]crdt.Track{}
	cut := s.nodes[0].n.cutoff()
	for _, sn := range s.nodes {
		for id, tr := range sn.n.tracks.Snapshot() {
			if tr.TS.WallMS < cut {
				continue
			}
			if cur, ok := want[id]; !ok || crdt.Compare(tr, cur) > 0 {
				want[id] = tr
			}
		}
	}
	for range 2 {
		for _, a := range s.nodes {
			for _, b := range s.nodes {
				if a == b {
					continue
				}
				if err := a.n.syncOnce(context.Background(), "http://"+b.id); err != nil {
					s.failf("settling: %s with %s: %v", a.id, b.id, err)
				}
			}
		}
	}
	s.check()
	for _, sn := range s.nodes {
		if got, exp := sn.n.Hash(), crdt.HashOf(want); got != exp {
			s.failf("after settling, node %s holds %d aircraft (%s), want the merge of everything held: %d (%s)",
				sn.id, sn.n.tracks.Len(), got[:12], len(want), exp[:12])
		}
	}
}

// simTransport is a node's network: it hands each request straight to the
// peer's handler, after letting the seed decide what goes wrong on the way.
type simTransport struct {
	s    *sim
	from string
}

func (tr simTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	s := tr.s
	body, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		return nil, err
	}
	to, path := r.URL.Host, r.URL.Path
	if s.cut[link(tr.from, to)] {
		return nil, errors.New("partitioned")
	}
	fault := 100
	if s.faulty {
		fault = s.rng.IntN(100)
	}
	if fault < 25 {
		s.faults++
		s.seen.faults[fault/5]++
	}
	if fault >= 15 && strings.HasSuffix(path, "/sync") {
		s.countTies(tr.from, to, body)
	}
	switch {
	case fault < 5:
		return nil, errors.New("request lost")
	case fault < 10:
		s.held = append(s.held, heldRequest{tr.from, to, path, body})
		return nil, errors.New("request delayed past the client timeout")
	case fault < 15:
		// The sender gives up mid-upload: the receiver reads a short body
		// and then a broken connection.
		short := io.MultiReader(bytes.NewReader(body[:s.rng.IntN(len(body))]), brokenConn{})
		aborted := s.byID[to].n.Counters().Aborted
		s.deliver(to, path, short).Body.Close()
		if s.byID[to].n.Counters().Aborted != aborted+1 {
			s.failf("a body cut off in flight was not counted as aborted")
		}
		return nil, errors.New("timed out mid-upload")
	case fault < 20:
		s.deliver(to, path, bytes.NewReader(body)).Body.Close()
		return nil, errors.New("response lost")
	case fault < 25:
		s.deliver(to, path, bytes.NewReader(body)).Body.Close()
		return s.deliver(to, path, bytes.NewReader(body)), nil
	default:
		return s.deliver(to, path, bytes.NewReader(body)), nil
	}
}

// countTies counts aircraft where a sync request's digest and the receiver
// hold the same stamp and origin with different contents.
func (s *sim) countTies(from, to string, body []byte) {
	var req syncRequest
	if json.Unmarshal(body, &req) != nil {
		return
	}
	for id, mine := range s.byID[to].n.tracks.Digest() {
		if v, ok := req.Digest[id]; ok && v.TS == mine.TS && v.Origin == mine.Origin && v.Sum != mine.Sum {
			s.seen.ties++
		}
	}
}

func (s *sim) deliver(to, path string, body io.Reader) *http.Response {
	rec := httptest.NewRecorder()
	s.byID[to].n.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "http://"+to+path, body))
	return rec.Result()
}

type brokenConn struct{}

func (brokenConn) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

// A node whose clock ran fast stamps a report ahead of true time, a peer
// stores it, then the node restarts with its clock corrected. Its fresh report
// of the same aircraft is stamped below the old one, so without care the
// older position wins everywhere, and nothing counts it as a lost write.
func TestRestartedNodesFreshReportWins(t *testing.T) {
	s := &sim{t: t, rng: rand.New(rand.NewPCG(1, 2)), trueMS: simStartMS, byID: map[string]*simNode{},
		cut: map[[2]string]bool{}, seen: &simCoverage{}}
	a, c := &simNode{id: "a"}, &simNode{id: "c", offsetMS: 2000}
	for _, sn := range []*simNode{a, c} {
		s.nodes = append(s.nodes, sn)
		s.byID[sn.id] = sn
		s.start(sn)
	}
	s.trueMS += 10_000 // past the startup hold
	old := crdt.Observation{Lat: 39, Lon: -77, ObservedAtMS: s.trueMS}
	c.n.Ingest("A00001", old)
	s.syncPair(c)

	c.offsetMS = 0 // the clock is corrected on reboot
	s.start(c)
	s.trueMS += 500
	fresh := crdt.Observation{Lat: 39.01, Lon: -77, ObservedAtMS: s.trueMS}
	c.n.Ingest("A00001", fresh)
	for range 3 {
		s.trueMS += 2500
		s.syncPair(c)
	}
	for _, sn := range s.nodes {
		if got := sn.n.tracks.Snapshot()["A00001"].Obs.ObservedAtMS; got != fresh.ObservedAtMS {
			t.Errorf("node %s holds the report from %d ms, want the fresh one from %d ms", sn.id, got, fresh.ObservedAtMS)
		}
	}
}
