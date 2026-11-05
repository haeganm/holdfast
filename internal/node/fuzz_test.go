package node

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/haeganm/holdfast/internal/crdt"
	"github.com/haeganm/holdfast/internal/hlc"
)

// FuzzHandlers sends arbitrary bodies to /sync and /push and checks what the
// trust boundary promises: no panic, only the expected statuses, every
// refusal counted exactly once, a refused message changes neither the
// replica nor the clock, a sync request never changes the responder's
// replica, and nothing invalid is ever stored.
//
//	go test -run '^$' -fuzz '^FuzzHandlers$' -fuzztime 30s ./internal/node
func FuzzHandlers(f *testing.F) {
	ts := hlc.Timestamp{WallMS: simStartMS}
	good := crdt.Track{Obs: obs(simStartMS), TS: ts, Origin: "b"}
	seed := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			f.Fatal(err)
		}
		return b
	}
	f.Add(false, seed(syncRequest{From: "b", HLC: ts, Digest: map[crdt.ICAO]crdt.Version{"AAAAA1": {TS: ts, Origin: "b"}}}))
	f.Add(true, seed(pushRequest{From: "b", HLC: ts, Tracks: map[crdt.ICAO]crdt.Track{"BBBBB1": good}}))
	f.Add(true, seed(pushRequest{From: "b", HLC: hlc.Timestamp{WallMS: simStartMS + 3_600_000}, Tracks: map[crdt.ICAO]crdt.Track{"BBBBB1": good}}))
	f.Add(true, []byte(`{"from":"b","hlc":{"w":1700000000000,"l":4294967295},"tracks":{}}`))
	f.Add(true, []byte(`{"from":"b","hlc":{"w":1,"l":0},"tracks":{"bbbbb1":{"obs":{"lat":91},"ts":{"w":1,"l":0},"origin":"b"}}}`))
	f.Add(false, []byte(`{"from":"b","hlc":{"w":1,"l":0},"digest":{"AAAAA1":{"ts":{"w":-1,"l":0},"origin":"","sum":0}}}`))
	f.Add(false, []byte(`{"from":"b","hlc":{"w":1e300}}`))
	f.Add(true, []byte(`{"from":"b"`))
	f.Add(false, []byte(``))

	f.Fuzz(func(t *testing.T, push bool, body []byte) {
		now := time.UnixMilli(simStartMS)
		n, err := New(Config{ID: "a", Now: func() time.Time { return now }, StartupHold: -1})
		if err != nil {
			t.Fatal(err)
		}
		n.Ingest("AAAAA1", obs(simStartMS))
		path := "/sync"
		if push {
			path = "/push"
		}
		hash, clock, before := n.Hash(), n.clock.Peek(), n.Counters()
		rec := httptest.NewRecorder()
		n.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
		after := n.Counters()

		switch {
		case !push && rec.Code == http.StatusOK:
			if n.Hash() != hash {
				t.Fatal("a sync request changed the responder's replica")
			}
			var resp syncResponse
			dec := json.NewDecoder(rec.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&resp); err != nil {
				t.Fatalf("the sync response does not decode: %v", err)
			}
			fallthrough
		case push && rec.Code == http.StatusNoContent:
			if after.Rejected != before.Rejected {
				t.Fatalf("accepted with status %d but counted as rejected", rec.Code)
			}
		case rec.Code == http.StatusBadRequest || rec.Code == http.StatusRequestEntityTooLarge:
			if after.Rejected != before.Rejected+1 {
				t.Fatalf("refused with status %d, rejected went from %d to %d", rec.Code, before.Rejected, after.Rejected)
			}
			if n.Hash() != hash {
				t.Fatal("a refused message changed the replica")
			}
			if n.clock.Peek() != clock {
				t.Fatalf("a refused message moved the clock from %v to %v", clock, n.clock.Peek())
			}
		default:
			t.Fatalf("status %d for %s", rec.Code, path)
		}

		limit := n.physNow().Add(n.maxDrift).UnixMilli()
		clk := n.clock.Peek()
		if clk.WallMS > limit {
			t.Fatalf("clock at %v, past the drift bound %d", clk, limit)
		}
		for id, tr := range n.tracks.Snapshot() {
			if err := checkTrack(id, tr, limit); err != nil {
				t.Fatalf("stored an invalid track %s: %v", id, err)
			}
			if tr.TS.Compare(clk) > 0 {
				t.Fatalf("stored %s at %v, above the clock %v", id, tr.TS, clk)
			}
		}
	})
}
