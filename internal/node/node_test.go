package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haeganm/holdfast/internal/crdt"
	"github.com/haeganm/holdfast/internal/hlc"
)

// testNode is a node behind a real HTTP server, with a switch that cuts it
// off from the network in both directions.
type testNode struct {
	*Node
	srv *httptest.Server
	cut atomic.Bool
}

type cutTransport struct {
	self *testNode
	base http.RoundTripper
}

func (c cutTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if c.self.cut.Load() {
		return nil, errors.New("partitioned")
	}
	resp, err := c.base.RoundTrip(r)
	if err == nil && c.self.cut.Load() { // a cut that lands mid-request loses the reply too
		resp.Body.Close()
		return nil, errors.New("partitioned")
	}
	return resp, err
}

type spec struct {
	id     string
	offset time.Duration
	now    func() time.Time // nil: the real clock
	ttl    time.Duration    // 0: the default track TTL
}

// startCluster starts fully meshed nodes that sync every 20 ms.
func startCluster(t *testing.T, wall bool, specs ...spec) map[string]*testNode {
	t.Helper()
	nodes := map[string]*testNode{}
	for _, s := range specs {
		tn := &testNode{}
		tn.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if tn.cut.Load() {
				http.Error(w, "partitioned", http.StatusServiceUnavailable)
				return
			}
			tn.Handler().ServeHTTP(w, r)
		}))
		t.Cleanup(tn.srv.Close)
		nodes[s.id] = tn
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for _, s := range specs {
		tn := nodes[s.id]
		var peers []string
		for _, o := range specs {
			if o.id != s.id {
				peers = append(peers, nodes[o.id].srv.URL)
			}
		}
		n, err := New(Config{
			ID:           s.id,
			Peers:        peers,
			SyncInterval: 20 * time.Millisecond,
			Wall:         wall,
			ClockOffset:  s.offset,
			Now:          s.now,
			TrackTTL:     s.ttl,
			StartupHold:  -1, // fresh nodes, clocks within the test's offsets
			Client:       &http.Client{Timeout: time.Second, Transport: cutTransport{tn, http.DefaultTransport}},
		})
		if err != nil {
			t.Fatal(err)
		}
		tn.Node = n
	}
	for _, tn := range nodes {
		go tn.Run(ctx)
	}
	return nodes
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func obs(at int64) crdt.Observation { return crdt.Observation{Lat: 39, Lon: -77, ObservedAtMS: at} }

func TestPartitionedNodeCatchesUpAfterHeal(t *testing.T) {
	ns := startCluster(t, false, spec{id: "a"}, spec{id: "b"}, spec{id: "c"})
	a, b, c := ns["a"], ns["b"], ns["c"]

	c.cut.Store(true)
	a.Ingest("AAAAA1", obs(1))
	a.Ingest("BBBBB1", obs(2))
	time.Sleep(5 * time.Millisecond) // c's write to the shared aircraft is later in true time
	c.Ingest("BBBBB1", obs(3))
	c.Ingest("CCCCC1", obs(4))

	waitFor(t, "a and b to converge while c is cut off", func() bool {
		return a.Hash() == b.Hash() && b.tracks.Len() == 2
	})
	time.Sleep(100 * time.Millisecond) // five sync intervals with no way through
	if c.tracks.Len() != 2 || c.Hash() == a.Hash() {
		t.Fatalf("c learned something through the partition: %d tracks", c.tracks.Len())
	}

	c.cut.Store(false)
	waitFor(t, "all three to converge after the heal", func() bool {
		return a.Hash() == b.Hash() && b.Hash() == c.Hash() && a.tracks.Len() == 3
	})
	if got := a.tracks.Snapshot()["BBBBB1"].Origin; got != "c" {
		t.Fatalf("BBBBB1 resolved to origin %q, want c (the later write)", got)
	}
	for id, tn := range ns {
		cs := tn.Counters()
		if cs.IngestLost != 0 || cs.MergeRegress != 0 {
			t.Fatalf("node %s: ingest_lost=%d merge_regress=%d, want 0 with synchronized clocks", id, cs.IngestLost, cs.MergeRegress)
		}
	}
	if c.Counters().SyncErr == 0 || a.Counters().SyncErr == 0 {
		t.Fatal("the partition produced no sync errors, so it was not a partition")
	}
}

// The claim the clock experiment measures, at unit scale: a node whose clock
// is 500 ms slow makes a fresher observation of an aircraft it has already
// heard about. Under the HLC the fresh one wins. Under the wall clock it loses
// to the stale one, on every node.
func TestSlowClockFreshWriteWinsOnlyUnderHLC(t *testing.T) {
	for _, tc := range []struct {
		name     string
		wall     bool
		wantLost int64
		winner   string
	}{
		{"hlc", false, 0, "c"},
		{"wall", true, 1, "a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns := startCluster(t, tc.wall, spec{id: "a"}, spec{id: "c", offset: -500 * time.Millisecond})
			a, c := ns["a"], ns["c"]
			a.Ingest("AAAAA1", obs(1))
			waitFor(t, "c to hear about the aircraft", func() bool { return c.tracks.Len() == 1 })
			c.Ingest("AAAAA1", obs(2))
			waitFor(t, "a and c to converge", func() bool { return a.Hash() == c.Hash() })
			if got := c.Counters().IngestLost; got != tc.wantLost {
				t.Fatalf("c ingest_lost = %d, want %d", got, tc.wantLost)
			}
			for id, tn := range ns {
				if got := tn.tracks.Snapshot()["AAAAA1"].Origin; got != tc.winner {
					t.Fatalf("node %s kept origin %q, want %q", id, got, tc.winner)
				}
			}
		})
	}
}

// The clock-skew runs give a node its clock error through ClockOffset, and the
// node's own clock is the only one a real node has. So the drift bound is
// measured on it: a node 10 s slow refuses a stamp 10 s ahead of its clock,
// though that stamp is true time, and takes one 4 s ahead.
func TestDriftIsMeasuredOnTheNodesOwnClock(t *testing.T) {
	n, err := New(Config{ID: "a", Now: func() time.Time { return time.UnixMilli(simStartMS) },
		ClockOffset: -10 * time.Second, StartupHold: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.receive(hlc.Timestamp{WallMS: simStartMS}, nil); err == nil {
		t.Fatal("a node 10 s slow accepted a stamp 10 s ahead of its own clock")
	}
	if err := n.receive(hlc.Timestamp{WallMS: simStartMS - 6000}, nil); err != nil {
		t.Fatalf("a stamp 4 s ahead of the node's own clock was refused: %v", err)
	}
}

// The startup hold lasts its full length on the node's own clock, the one its
// stamps come from. Timed against true time, a node 3 s slow would end a 5 s
// hold after 2 s.
func TestTheStartupHoldRunsOnTheNodesOwnClock(t *testing.T) {
	now := time.UnixMilli(simStartMS)
	n, err := New(Config{ID: "a", Now: func() time.Time { return now }, ClockOffset: -3 * time.Second, StartupHold: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(4 * time.Second)
	n.Ingest("AAAAA1", obs(simStartMS+4000))
	if n.tracks.Len() != 0 {
		t.Fatal("a report was stamped 4 s into a 5 s startup hold")
	}
	now = now.Add(time.Second)
	n.Ingest("AAAAA2", obs(simStartMS+5000))
	if n.tracks.Len() != 2 {
		t.Fatalf("%d aircraft stored 5 s after the start, want both: the hold is over", n.tracks.Len())
	}
}

// now_ms is the harness's time base for every staleness and lag number, so
// /state reports true time whatever the node's own clock reads.
func TestStateReportsTrueTime(t *testing.T) {
	n, err := New(Config{ID: "a", Now: func() time.Time { return time.UnixMilli(simStartMS) },
		ClockOffset: 2 * time.Second, StartupHold: -1})
	if err != nil {
		t.Fatal(err)
	}
	if got := n.Snapshot().NowMS; got != simStartMS {
		t.Fatalf("now_ms = %d with the clock 2 s fast, want true time %d", got, int64(simStartMS))
	}
}

func newLoneNode(t *testing.T) (*Node, *httptest.Server) {
	t.Helper()
	n, err := New(Config{ID: "a", StartupHold: -1})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(n.Handler())
	t.Cleanup(srv.Close)
	return n, srv
}

func TestTrustBoundaryRejectsBadMessages(t *testing.T) {
	n, srv := newLoneNode(t)
	nowMS := time.Now().UnixMilli()
	good := crdt.Track{Obs: obs(1), TS: hlc.Timestamp{WallMS: nowMS}, Origin: "b"}
	push := func(tracks map[crdt.ICAO]crdt.Track, ts hlc.Timestamp) []byte {
		body, err := json.Marshal(pushRequest{From: "b", HLC: ts, Tracks: tracks})
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	far := hlc.Timestamp{WallMS: nowMS + time.Hour.Milliseconds()}
	wrap := hlc.Timestamp{WallMS: nowMS, Logical: math.MaxUint32}
	tooMany := map[crdt.ICAO]crdt.Track{}
	tooManyVersions := map[crdt.ICAO]crdt.Version{}
	for i := range maxEntries + 1 {
		tooMany[crdt.ICAO(fmt.Sprintf("%06X", i))] = good
		tooManyVersions[crdt.ICAO(fmt.Sprintf("%06X", i))] = crdt.Version{TS: good.TS, Origin: "b"}
	}
	withTS := func(tr crdt.Track, ts hlc.Timestamp) crdt.Track { tr.TS = ts; return tr }
	withLat := func(tr crdt.Track, lat float64) crdt.Track { tr.Obs.Lat = lat; return tr }
	withLon := func(tr crdt.Track, lon float64) crdt.Track { tr.Obs.Lon = lon; return tr }
	withOrigin := func(tr crdt.Track, o string) crdt.Track { tr.Origin = o; return tr }
	now := hlc.Timestamp{WallMS: nowMS}
	digest, err := json.Marshal(syncRequest{From: "b", HLC: now, Digest: tooManyVersions})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		path string
		body []byte
		want int
	}{
		{"lowercase aircraft id", "/push", push(map[crdt.ICAO]crdt.Track{"abcdef": good}, now), 400},
		{"track stamp an hour ahead", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": withTS(good, far)}, now), 400},
		{"message stamp an hour ahead", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": good}, far), 400},
		{"negative track stamp", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": withTS(good, hlc.Timestamp{WallMS: -5})}, now), 400},
		{"logical part that would wrap the clock", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": withTS(good, wrap)}, now), 400},
		{"more tracks than one message may carry", "/push", push(tooMany, now), 400},
		{"latitude out of range", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": withLat(good, 200)}, now), 400},
		{"longitude out of range", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": withLon(good, 200)}, now), 400},
		{"bad origin", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": withOrigin(good, "B; DROP")}, now), 400},
		{"unknown field", "/push", []byte(`{"from":"b","hlc":{"w":1,"l":0},"tracks":{},"admin":true}`), 400},
		{"bad sender id", "/sync", []byte(`{"from":"","hlc":{"w":1,"l":0},"digest":{}}`), 400},
		{"digest with a bad id", "/sync", []byte(`{"from":"b","hlc":{"w":1,"l":0},"digest":{"zz":{"ts":{"w":1,"l":0},"origin":"b","sum":0}}}`), 400},
		{"digest with a bad origin", "/sync", []byte(`{"from":"b","hlc":{"w":1,"l":0},"digest":{"ABCDEF":{"ts":{"w":1,"l":0},"origin":"B; DROP","sum":0}}}`), 400},
		{"more digest entries than one message may carry", "/sync", digest, 400},
		{"body over 4 MB", "/push", append([]byte(`{"from":"b","tracks":{},"pad":"`), bytes.Repeat([]byte("x"), maxBody)...), 413},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Post(srv.URL+tc.path, "application/json", bytes.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			msg, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status %d (%s), want %d", resp.StatusCode, bytes.TrimSpace(msg), tc.want)
			}
			if n.tracks.Len() != 0 {
				t.Fatal("a rejected message changed the replica")
			}
			if got := n.Counters().Rejected; got != int64(i+1) {
				t.Fatalf("rejected = %d, want %d", got, i+1)
			}
		})
	}

	resp, err := http.Post(srv.URL+"/push", "application/json", bytes.NewReader(push(map[crdt.ICAO]crdt.Track{"ABCDEF": good}, now)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || n.tracks.Len() != 1 {
		t.Fatalf("a valid push got %d and left %d tracks, want 204 and 1", resp.StatusCode, n.tracks.Len())
	}
}

// A peer's message stamp can be older than a track it carries (the push stamp
// is taken before the tracks are gathered, and a merge can land in between).
// The clock must still move past the track, or the next local write loses.
func TestClockAdvancesPastTrackStampsNotJustTheMessageStamp(t *testing.T) {
	n, _ := newLoneNode(t)
	now := time.Now().UnixMilli()
	ahead := hlc.Timestamp{WallMS: now + 1000}
	err := n.receive(hlc.Timestamp{WallMS: now - 1000}, map[crdt.ICAO]crdt.Track{
		"AAAAA1": {Obs: obs(1), TS: ahead, Origin: "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Ingest("AAAAA1", obs(2))
	if got := n.Counters().IngestLost; got != 0 {
		t.Fatalf("ingest_lost = %d: the local write was stamped below a track the node had already merged", got)
	}
	if got := n.tracks.Snapshot()["AAAAA1"].Origin; got != "a" {
		t.Fatalf("kept origin %q, want the local write", got)
	}
}

// A local write must take its stamp while holding the node lock that receive
// holds while it advances the clock and merges. Otherwise a merge can land
// between the stamp and the store, and the fresh write loses to it. The clock
// is the hook: it runs at the exact moment a stamp is taken, so it can check
// who holds the lock. Both paths that stamp are covered: a plain write, and
// the reports a node held during its startup hold.
func TestIngestStampsUnderTheLock(t *testing.T) {
	for _, hold := range []time.Duration{-1, time.Second} {
		var n *Node
		var unlocked atomic.Int32
		now := time.UnixMilli(simStartMS)
		clock := func() time.Time {
			if n != nil && n.mu.TryLock() {
				n.mu.Unlock()
				unlocked.Add(1)
			}
			return now
		}
		var err error
		n, err = New(Config{ID: "a", Now: clock, StartupHold: hold})
		if err != nil {
			t.Fatal(err)
		}
		n.Ingest("AAAAA1", obs(1))
		now = now.Add(2 * time.Second)
		n.Ingest("AAAAA2", obs(2))
		if n.tracks.Len() != 2 {
			t.Fatalf("hold %v: %d tracks stored, want 2", hold, n.tracks.Len())
		}
		if got := unlocked.Load(); got != 0 {
			t.Fatalf("hold %v: %d clock readings taken without the node lock", hold, got)
		}
	}
}

// A stamp exactly at the logical bound is legal. The node that receives it
// must not then emit stamps over the bound, or every honest peer rejects it.
func TestStampAtTheLogicalBoundDoesNotCutTheNodeOff(t *testing.T) {
	n, _ := newLoneNode(t)
	peer, _ := newLoneNode(t)
	at := hlc.Timestamp{WallMS: time.Now().UnixMilli() + 1000, Logical: hlc.MaxLogical}
	if err := n.receive(at, map[crdt.ICAO]crdt.Track{"AAAAA1": {Obs: obs(1), TS: at, Origin: "b"}}); err != nil {
		t.Fatal(err)
	}
	n.Ingest("AAAAA1", obs(2))
	mine := n.tracks.Snapshot()["AAAAA1"]
	if mine.Origin != "a" || n.Counters().IngestLost != 0 {
		t.Fatalf("local write lost to the stamp at the bound: kept %+v", mine)
	}
	if err := peer.receive(n.clock.Now(), map[crdt.ICAO]crdt.Track{"AAAAA1": mine}); err != nil {
		t.Fatalf("a peer rejects this node after it saw a stamp at the bound: %v", err)
	}
}

// merge_regress feeds the clock-skew figure, so it has to count, not just
// stay at zero: a received track that wins by stamp but carries an older
// observation than the one it replaces is one regress.
func TestMergeRegressCountsAStaleWinner(t *testing.T) {
	n, _ := newLoneNode(t)
	now := time.Now().UnixMilli()
	for _, tr := range []crdt.Track{
		{Obs: obs(2), TS: hlc.Timestamp{WallMS: now - 100}, Origin: "b"}, // the fresher report, lower stamp
		{Obs: obs(1), TS: hlc.Timestamp{WallMS: now}, Origin: "c"},       // older report, higher stamp: wins wrongly
		{Obs: obs(3), TS: hlc.Timestamp{WallMS: now + 1}, Origin: "c"},   // fresher and higher: a correct win
	} {
		if err := n.receive(tr.TS, map[crdt.ICAO]crdt.Track{"AAAAA1": tr}); err != nil {
			t.Fatal(err)
		}
	}
	c := n.Counters()
	if c.MergeApplied != 3 || c.MergeRegress != 1 {
		t.Fatalf("merge_applied=%d merge_regress=%d, want 3 and 1", c.MergeApplied, c.MergeRegress)
	}
}

// ingest_lost is the clock-skew sweep's headline for the wall-clock baseline,
// so it must count only a local report that lost while fresher than what is
// stored. An older report losing to a fresher one is the merge being right.
func TestIngestLostCountsOnlyAFresherReportLosing(t *testing.T) {
	n, err := New(Config{ID: "a", Wall: true, Now: func() time.Time { return time.UnixMilli(simStartMS) }})
	if err != nil {
		t.Fatal(err)
	}
	stored := crdt.Track{Obs: obs(simStartMS + 500), TS: hlc.Timestamp{WallMS: simStartMS + 1000}, Origin: "b"}
	if err := n.receive(stored.TS, map[crdt.ICAO]crdt.Track{"AAAAA1": stored}); err != nil {
		t.Fatal(err)
	}
	n.Ingest("AAAAA1", obs(simStartMS)) // older than what is stored, and stamped below it
	if got := n.Counters().IngestLost; got != 0 {
		t.Fatalf("ingest_lost = %d after an older report lost to a fresher one", got)
	}
	n.Ingest("AAAAA1", obs(simStartMS+900)) // fresher, and still stamped below it
	if got := n.Counters().IngestLost; got != 1 {
		t.Fatalf("ingest_lost = %d after a fresher report lost, want 1", got)
	}
}

// merge_applied is the denominator of the published bytes per useful update,
// so a received track that loses, or is a redelivery, counts as stale.
func TestALosingOrRedeliveredTrackIsStaleNotApplied(t *testing.T) {
	n, _ := newLoneNode(t)
	now := time.Now().UnixMilli()
	tr := crdt.Track{Obs: obs(2), TS: hlc.Timestamp{WallMS: now}, Origin: "b"}
	older := crdt.Track{Obs: obs(1), TS: hlc.Timestamp{WallMS: now - 1}, Origin: "b"}
	for _, t2 := range []crdt.Track{tr, tr, older} {
		if err := n.receive(t2.TS, map[crdt.ICAO]crdt.Track{"AAAAA1": t2}); err != nil {
			t.Fatal(err)
		}
	}
	if c := n.Counters(); c.MergeApplied != 1 || c.MergeStale != 2 {
		t.Fatalf("merge_applied=%d merge_stale=%d after a delivery, a redelivery and a loser, want 1 and 2", c.MergeApplied, c.MergeStale)
	}
}

// A host that restores tracks into a fresh node (the cloud peer does, on every
// request) must get a clock past them, or the node's next stamp could sort
// below what it just loaded. Invalid tracks are refused, not loaded.
func TestRestoreAdvancesTheClockAndRefusesBadTracks(t *testing.T) {
	n, _ := newLoneNode(t)
	ahead := hlc.Timestamp{WallMS: time.Now().UnixMilli() + 3000, Logical: 7}
	refused := n.Restore(map[crdt.ICAO]crdt.Track{
		"AAAAA1": {Obs: obs(1), TS: ahead, Origin: "b"},
		"bad":    {Obs: obs(1), TS: ahead, Origin: "b"},
		"AAAAA2": {Obs: obs(1), TS: hlc.Timestamp{WallMS: time.Now().UnixMilli() + time.Hour.Milliseconds()}, Origin: "b"},
	})
	if len(refused) != 2 || len(n.Tracks()) != 1 {
		t.Fatalf("refused %v, kept %d; want 2 and 1", refused, len(n.Tracks()))
	}
	if now := n.clock.Now(); now.Compare(ahead) <= 0 {
		t.Fatalf("clock at %v after restoring a track at %v", now, ahead)
	}
}

// Nobody authenticates a peer, so the replica bounds itself. Once it holds
// maxEntries aircraft, a track for one it doesn't hold is refused and counted,
// whether a host restores it, a peer sends it or the node's own sensor reports
// it, and the aircraft it holds still update.
func TestAFullReplicaRefusesNewAircraft(t *testing.T) {
	ts := hlc.Timestamp{WallMS: simStartMS}
	aircraft := func(k int) map[crdt.ICAO]crdt.Track {
		m := make(map[crdt.ICAO]crdt.Track, k)
		for i := range k {
			m[crdt.ICAO(fmt.Sprintf("%06X", i))] = crdt.Track{Obs: obs(1), TS: ts, Origin: "b"}
		}
		return m
	}
	newNode := func() *Node {
		n, err := New(Config{ID: "a", Now: func() time.Time { return time.UnixMilli(simStartMS) }, StartupHold: -1})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	n := newNode()
	if refused := n.Restore(aircraft(maxEntries + 1)); len(refused) != 0 || n.tracks.Len() != maxEntries || n.Counters().Capped != 1 {
		t.Fatalf("restoring %d valid aircraft refused %v, kept %d, capped %d; want none refused, %d kept, 1 capped",
			maxEntries+1, refused, n.tracks.Len(), n.Counters().Capped, maxEntries)
	}

	n = newNode()
	if err := n.receive(ts, aircraft(maxEntries)); err != nil {
		t.Fatal(err)
	}
	later := hlc.Timestamp{WallMS: simStartMS + 1}
	if err := n.receive(later, map[crdt.ICAO]crdt.Track{
		"FFFFF0": {Obs: obs(2), TS: later, Origin: "b"}, // new
		"000000": {Obs: obs(2), TS: later, Origin: "b"}, // held
	}); err != nil {
		t.Fatal(err)
	}
	n.Ingest("FFFFF1", obs(3)) // new
	n.Ingest("000001", obs(3)) // held
	snap := n.tracks.Snapshot()
	if c := n.Counters(); len(snap) != maxEntries || c.Capped != 2 {
		t.Fatalf("%d aircraft and capped %d after a peer and the sensor each sent a new one; want %d and 2", len(snap), c.Capped, maxEntries)
	}
	if snap["000000"].TS != later || snap["000001"].Origin != "a" {
		t.Fatalf("an aircraft the full replica holds did not update: %+v, %+v", snap["000000"], snap["000001"])
	}
	if _, ok := n.own["FFFFF1"]; ok {
		t.Fatal("the node counts a report it refused among its own, which would grow without bound")
	}
}

func TestMetricsExposition(t *testing.T) {
	n, srv := newLoneNode(t)
	n.Ingest("ABCDEF", obs(1))
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		"# TYPE holdfast_tracks gauge\nholdfast_tracks 1\n",
		"holdfast_ingest_total 1\n",
		"holdfast_capped_total 0\n",
		`holdfast_sync_requests_total{result="err"} 0` + "\n",
		"# TYPE holdfast_sync_bytes_total counter\n",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}
}

func TestStateReportsOwnObservations(t *testing.T) {
	n, srv := newLoneNode(t)
	n.Ingest("ABCDEF", obs(42))
	resp, err := http.Get(srv.URL + "/state")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st State
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	tr := st.Tracks["ABCDEF"]
	if st.ID != "a" || tr.At != 42 || tr.Own != 42 || tr.Origin != "a" || st.Hash != n.Hash() {
		t.Fatalf("unexpected state %+v", st)
	}
}

// Under packet loss a sender's 2 s timeout can fire mid-upload, and the
// receiver sees a body that stops short. That is the network failing, not a
// peer sending something bad, so it must not count against the trust boundary.
// The same broken JSON delivered whole is still a rejection.
func TestCutOffBodyIsAbortedNotRejected(t *testing.T) {
	part := `{"from":"b","hlc":`
	for _, tc := range []struct {
		name          string
		contentLength int
		rejected      int64
		aborted       int64
	}{
		{"connection drops mid-body", 1000, 0, 1},
		{"complete body, truncated JSON", len(part), 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, srv := newLoneNode(t)
			conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(conn, "POST /push HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", tc.contentLength, part)
			if tc.aborted > 0 {
				conn.Close()
			} else {
				defer conn.Close()
			}
			waitFor(t, "the server to give up on the body", func() bool {
				c := n.Counters()
				return c.Rejected+c.Aborted > 0
			})
			if c := n.Counters(); c.Rejected != tc.rejected || c.Aborted != tc.aborted {
				t.Fatalf("rejected=%d aborted=%d, want %d and %d", c.Rejected, c.Aborted, tc.rejected, tc.aborted)
			}
		})
	}
}

// During the startup hold a node keeps its reports, the latest per aircraft
// whatever order they arrive in, and stamps them as soon as the hold ends,
// above every stamp that existed when it started.
func TestStartupHoldKeepsTheLatestReportAndStampsItAfterTheHold(t *testing.T) {
	now := time.UnixMilli(simStartMS)
	n, err := New(Config{ID: "a", Now: func() time.Time { return now }, StartupHold: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	n.Ingest("AAAAA1", obs(simStartMS+200))
	n.Ingest("AAAAA1", obs(simStartMS+100)) // late arrival of an older report
	n.Ingest("BBBBB1", obs(simStartMS+300))
	if n.tracks.Len() != 0 {
		t.Fatal("a report entered the replica during the hold")
	}
	now = now.Add(5 * time.Second)
	n.Ingest("CCCCC1", obs(simStartMS+5000)) // the first report after the hold releases the rest
	snap := n.tracks.Snapshot()
	if got := snap["AAAAA1"].Obs.ObservedAtMS; got != simStartMS+200 {
		t.Fatalf("AAAAA1 holds the report from %d, want the latest, from %d", got, simStartMS+200)
	}
	if len(snap) != 3 {
		t.Fatalf("%d aircraft after the hold, want 3", len(snap))
	}
	for id, tr := range snap {
		if tr.TS.WallMS < simStartMS+5000 {
			t.Errorf("%s stamped at %v, inside the hold", id, tr.TS)
		}
	}
	if c := n.Counters(); c.IngestHeld != 3 || c.IngestTotal != 4 {
		t.Fatalf("ingest_held=%d ingest_total=%d, want 3 and 4", c.IngestHeld, c.IngestTotal)
	}
}

// The hold makes a node's own reports late. A held report stamped over a
// fresher observation a peer delivered meanwhile would sort above it on every
// node and move the picture back, so it is skipped. A held report fresher than
// what the peer delivered is still stamped.
func TestTheHoldNeverStampsAReportOverAFresherOne(t *testing.T) {
	now := time.UnixMilli(simStartMS)
	n, err := New(Config{ID: "a", Now: func() time.Time { return now }, StartupHold: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(100 * time.Millisecond)
	n.Ingest("AAAAA1", obs(simStartMS+100))
	now = now.Add(1900 * time.Millisecond)
	n.Ingest("BBBBB1", obs(simStartMS+2000))
	now = now.Add(time.Second)
	peer := hlc.Timestamp{WallMS: simStartMS + 3000}
	if err := n.receive(peer, map[crdt.ICAO]crdt.Track{
		"AAAAA1": {Obs: obs(simStartMS + 3000), TS: peer, Origin: "b"}, // fresher than the held report
		"BBBBB1": {Obs: obs(simStartMS + 1000), TS: peer, Origin: "b"}, // older than the held report
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	n.Ingest("CCCCC1", obs(simStartMS+5000)) // the first report after the hold releases the rest
	snap := n.tracks.Snapshot()
	if got := snap["AAAAA1"]; got.Origin != "b" {
		t.Errorf("the hold ended by stamping the report observed at +%d ms over the peer's from +3000 ms", got.Obs.ObservedAtMS-simStartMS)
	}
	if got := snap["BBBBB1"]; got.Origin != "a" {
		t.Errorf("the held report from +2000 ms was dropped for the peer's from +%d ms", got.Obs.ObservedAtMS-simStartMS)
	}
	if c := n.Counters(); c.IngestRegress != 0 || len(snap) != 3 {
		t.Errorf("ingest_regress=%d with %d aircraft, want 0 and 3", c.IngestRegress, len(snap))
	}
}

// The hold also ends on a sync tick, so a node that goes quiet after starting
// doesn't sit on its reports.
func TestStartupHoldEndsOnASyncTick(t *testing.T) {
	now := time.UnixMilli(simStartMS)
	n, err := New(Config{ID: "a", Now: func() time.Time { return now }, StartupHold: time.Second,
		Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("no peer")
		})}})
	if err != nil {
		t.Fatal(err)
	}
	n.Ingest("AAAAA1", obs(simStartMS))
	now = now.Add(time.Second)
	_ = n.syncOnce(context.Background(), "http://b")
	if n.tracks.Len() != 1 {
		t.Fatal("the held report was not stamped on the first tick after the hold")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Last-writer-wins orders writes, not observations: a sensor that reports an
// older position after a peer delivered a newer one moves the picture back.
// The node can't stop that without changing the merge rule, but it counts it.
func TestIngestRegressCountsAnOlderLocalReportWinning(t *testing.T) {
	n, _ := newLoneNode(t)
	newer := crdt.Track{Obs: obs(200), TS: hlc.Timestamp{WallMS: time.Now().UnixMilli()}, Origin: "b"}
	if err := n.receive(newer.TS, map[crdt.ICAO]crdt.Track{"AAAAA1": newer}); err != nil {
		t.Fatal(err)
	}
	n.Ingest("AAAAA1", obs(100)) // the local sensor heard an older position
	n.Ingest("AAAAA2", obs(100)) // a first report regresses nothing
	n.Ingest("AAAAA2", obs(300)) // and neither does a fresher one
	if got := n.tracks.Snapshot()["AAAAA1"].Obs.ObservedAtMS; got != 100 {
		t.Fatalf("stored the report from %d; last-writer-wins should have kept the local write", got)
	}
	if c := n.Counters(); c.IngestRegress != 1 || c.IngestLost != 0 {
		t.Fatalf("ingest_regress=%d ingest_lost=%d, want 1 and 0", c.IngestRegress, c.IngestLost)
	}
}

// An aircraft restored from storage after its track expired stays out, as it
// would from a peer, and isn't reported as invalid: nothing needs repairing.
func TestRestoreLeavesOutExpiredTracks(t *testing.T) {
	now := time.UnixMilli(simStartMS)
	n, err := New(Config{ID: "cloud", Now: func() time.Time { return now }, StartupHold: -1, TrackTTL: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	old := crdt.Track{Obs: obs(1), TS: hlc.Timestamp{WallMS: simStartMS - time.Hour.Milliseconds()}, Origin: "b"}
	live := crdt.Track{Obs: obs(2), TS: hlc.Timestamp{WallMS: simStartMS}, Origin: "b"}
	refused := n.Restore(map[crdt.ICAO]crdt.Track{"AAAAA1": old, "AAAAA2": live})
	if len(refused) != 0 || len(n.Tracks()) != 1 {
		t.Fatalf("refused %v and kept %d, want none refused and only the live one kept", refused, len(n.Tracks()))
	}
}

// A partition longer than the track TTL. Each side expires what it stopped
// hearing about, as a lone node would. At the heal the fresh versions merge,
// and a version one side has already expired is refused, so nothing comes
// back; until the slower clock expires it too, the two disagree on that track.
func TestAPartitionLongerThanTheTTLBringsNothingBack(t *testing.T) {
	var nowMS atomic.Int64
	nowMS.Store(simStartMS)
	now := func() time.Time { return time.UnixMilli(nowMS.Load()) }
	ns := startCluster(t, false, spec{id: "a", offset: -2 * time.Second, now: now, ttl: time.Minute},
		spec{id: "b", now: now, ttl: time.Minute})
	a, b := ns["a"], ns["b"]
	a.Ingest("BBBBB1", obs(2)) // a keeps hearing this one through the cut
	a.Ingest("AAAAA1", obs(1)) // reported once, then never again
	waitFor(t, "b to learn both aircraft", func() bool { return b.tracks.Len() == 2 })
	// a's stamps only go up, so AAAAA1's is the later of the two, however far
	// a sync from b moved a's clock between the ingests.
	stamp := a.tracks.Snapshot()["AAAAA1"].TS.WallMS

	b.cut.Store(true)
	// Past both on b's clock; not past AAAAA1 on a's, which runs 2 s slow.
	nowMS.Store(stamp + time.Minute.Milliseconds() + 1000)
	a.Ingest("BBBBB1", obs(3))
	waitFor(t, "b, cut off, to expire both aircraft", func() bool { return b.tracks.Len() == 0 })

	b.cut.Store(false)
	waitFor(t, "b to take the fresh report and refuse the version it had expired", func() bool {
		return b.tracks.Len() == 1 && b.Counters().MergeExpired > 0
	})
	if _, back := b.tracks.Snapshot()["AAAAA1"]; back {
		t.Fatal("an aircraft b had expired came back after the heal")
	}
	nowMS.Store(stamp + time.Minute.Milliseconds() + 3000) // a's clock gets there too
	waitFor(t, "the two to agree once a expires it as well", func() bool {
		return a.tracks.Len() == 1 && a.Hash() == b.Hash()
	})
}

// A node with no peers has no sync rounds, and must still expire.
func TestALoneNodeStillExpires(t *testing.T) {
	var nowMS atomic.Int64
	nowMS.Store(simStartMS)
	n, err := New(Config{ID: "a", Now: func() time.Time { return time.UnixMilli(nowMS.Load()) }, StartupHold: -1,
		TrackTTL: time.Minute, SyncInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	n.Ingest("AAAAA1", obs(simStartMS))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)
	nowMS.Add(time.Hour.Milliseconds())
	waitFor(t, "the lone node to expire its aircraft", func() bool { return n.tracks.Len() == 0 })
}

// An expire that lands while a receive is under way must not let the receive
// bring the version back. The test holds the node lock, starts a receive that
// redelivers a version, and lets it run until it waits on the lock. Still
// holding the lock, it moves time past the version's expiry and expires it,
// then lets go. A receive that read its expiry cutoff before taking the lock
// now merges with a stale cutoff and brings the version back; one that reads
// it under the lock refuses it.
func TestAnExpireDuringAReceiveCannotBringAVersionBack(t *testing.T) {
	const ttl = 10 * time.Second
	var nowMS atomic.Int64
	validated := make(chan struct{}, 1)
	var armed atomic.Bool
	clock := func() time.Time {
		if armed.CompareAndSwap(true, false) { // receive's first read: validate
			validated <- struct{}{}
		}
		return time.UnixMilli(nowMS.Load())
	}
	nowMS.Store(simStartMS)
	n, err := New(Config{ID: "a", Now: clock, StartupHold: -1, TrackTTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	v := crdt.Track{Obs: obs(1), TS: hlc.Timestamp{WallMS: simStartMS - ttl.Milliseconds()}, Origin: "b"} // live until now
	n.tracks.Put("AAAAA1", v)

	n.mu.Lock()
	armed.Store(true)
	done := make(chan error)
	go func() { done <- n.receive(hlc.Timestamp{WallMS: simStartMS}, map[crdt.ICAO]crdt.Track{"AAAAA1": v}) }()
	<-validated
	time.Sleep(50 * time.Millisecond) // long enough to reach the lock, by orders of magnitude
	nowMS.Add(1)                      // v has now expired
	n.expireLocked()
	n.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, held := n.Tracks()["AAAAA1"]; held {
		t.Fatalf("the node holds a version stamped %v, below its own cutoff %d", v.TS, n.cut.Load())
	}
}
