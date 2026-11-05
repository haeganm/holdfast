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
	return c.base.RoundTrip(r)
}

type spec struct {
	id     string
	offset time.Duration
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
	for i := range maxEntries + 1 {
		tooMany[crdt.ICAO(fmt.Sprintf("%06X", i))] = good
	}
	withTS := func(tr crdt.Track, ts hlc.Timestamp) crdt.Track { tr.TS = ts; return tr }
	withLat := func(tr crdt.Track, lat float64) crdt.Track { tr.Obs.Lat = lat; return tr }
	withOrigin := func(tr crdt.Track, o string) crdt.Track { tr.Origin = o; return tr }
	now := hlc.Timestamp{WallMS: nowMS}

	cases := []struct {
		name string
		path string
		body []byte
		want int
	}{
		{"lowercase aircraft id", "/push", push(map[crdt.ICAO]crdt.Track{"abcdef": good}, now), 400},
		{"track stamp an hour ahead", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": withTS(good, far)}, now), 400},
		{"message stamp an hour ahead", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": good}, far), 400},
		{"logical part that would wrap the clock", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": withTS(good, wrap)}, now), 400},
		{"more tracks than one message may carry", "/push", push(tooMany, now), 400},
		{"latitude out of range", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": withLat(good, 200)}, now), 400},
		{"bad origin", "/push", push(map[crdt.ICAO]crdt.Track{"ABCDEF": withOrigin(good, "B; DROP")}, now), 400},
		{"unknown field", "/push", []byte(`{"from":"b","hlc":{"w":1,"l":0},"tracks":{},"admin":true}`), 400},
		{"bad sender id", "/sync", []byte(`{"from":"","hlc":{"w":1,"l":0},"digest":{}}`), 400},
		{"digest with a bad id", "/sync", []byte(`{"from":"b","hlc":{"w":1,"l":0},"digest":{"zz":{"ts":{"w":1,"l":0},"origin":"b","sum":0}}}`), 400},
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
