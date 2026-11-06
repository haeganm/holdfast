package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/haeganm/holdfast/internal/crdt"
	"github.com/haeganm/holdfast/internal/hlc"
	"github.com/haeganm/holdfast/internal/node"
)

const token = "0123456789abcdef0123456789abcdef"

var tables atomic.Int64

// newPeer returns a peer on a fresh table in DynamoDB Local at
// HOLDFAST_DYNAMODB. check.sh always starts one, and sets
// HOLDFAST_REQUIRE_DYNAMODB so these tests can't quietly skip there.
func newPeer(t *testing.T) *peer {
	t.Helper()
	endpoint := os.Getenv("HOLDFAST_DYNAMODB")
	if endpoint == "" {
		if os.Getenv("HOLDFAST_REQUIRE_DYNAMODB") != "" {
			t.Fatal("HOLDFAST_DYNAMODB is not set but HOLDFAST_REQUIRE_DYNAMODB is")
		}
		t.Skip("set HOLDFAST_DYNAMODB to a DynamoDB Local endpoint to run the store tests")
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")))
	if err != nil {
		t.Fatal(err)
	}
	db := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(endpoint) })
	table := fmt.Sprintf("holdfast-test-%d-%d", time.Now().UnixNano(), tables.Add(1))
	if err := ensureTable(context.Background(), db, table); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: &table}) })
	return &peer{db: db, table: table, token: token, ttl: 5 * time.Minute, now: time.Now,
		log: slog.New(slog.DiscardHandler)}
}

func call(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func track(at int64, origin string) crdt.Track {
	return crdt.Track{Obs: crdt.Observation{Lat: 39, Lon: -77, ObservedAtMS: at},
		TS: hlc.Timestamp{WallMS: time.Now().UnixMilli() - 1000 + at}, Origin: origin}
}

func push(t *testing.T, h http.Handler, tracks map[crdt.ICAO]crdt.Track) {
	t.Helper()
	rec := call(t, h, http.MethodPost, "/push", map[string]any{
		"from": "b", "hlc": hlc.Timestamp{WallMS: time.Now().UnixMilli()}, "tracks": tracks})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("push: %d %s", rec.Code, rec.Body)
	}
}

func stored(t *testing.T, p *peer) map[crdt.ICAO]crdt.Track {
	t.Helper()
	got, _, bad, err := p.load(context.Background())
	if err != nil || len(bad) != 0 {
		t.Fatalf("load: %v, bad items %v", err, bad)
	}
	return got
}

func TestPeerStatsCountTheWork(t *testing.T) {
	p := newPeer(t)
	first := track(1, "b")
	push(t, p, map[crdt.ICAO]crdt.Track{"AAAAA1": first, "AAAAA2": track(2, "b")})
	push(t, p, map[crdt.ICAO]crdt.Track{"AAAAA1": first}) // a redelivery writes nothing
	var s map[string]int64
	if err := json.Unmarshal(call(t, p, http.MethodGet, "/peer", nil).Body.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	if s["requests"] != 2 || s["writes"] != 2 || s["won"] != 2 || s["items_read"] != 2 || s["failed"] != 0 {
		t.Fatalf("stats %v", s)
	}
}

func TestTokenIsRequired(t *testing.T) {
	p := &peer{token: token, log: slog.New(slog.DiscardHandler)}
	for _, auth := range []string{"", "Bearer wrong", "Bearer " + token + "x", token} {
		req := httptest.NewRequest(http.MethodGet, "/hash", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Authorization %q got %d, want 401", auth, rec.Code)
		}
	}
}

// In Lambda the token is all that keeps the internet out of the table, so a
// short one must stop the function at start. Terraform checks it too, but
// only on its own deploy path.
func TestLambdaRefusesAShortToken(t *testing.T) {
	t.Setenv("TABLE", "holdfast")
	t.Setenv("AWS_PROFILE", "") // nothing on this machine may make the SDK's config fail instead
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "none"))
	for _, c := range []struct {
		token        string
		lambda, want bool
	}{
		{"", true, false},
		{token[:31], true, false},
		{token, true, true},
		{"", false, true}, // a local run may go without one, and warns
	} {
		t.Setenv("PEER_TOKEN", c.token)
		_, err := fromEnv(context.Background(), slog.New(slog.DiscardHandler), c.lambda)
		if c.want && err != nil || !c.want && (err == nil || !strings.Contains(err.Error(), "PEER_TOKEN")) {
			t.Errorf("a %d-character token, in Lambda %v: err %v, want accepted %v", len(c.token), c.lambda, err, c.want)
		}
	}
}

// A table that can't be read answers 503, not an empty picture.
func TestATableThatCantBeReadAnswers503(t *testing.T) {
	p := newPeer(t)
	gone := &peer{db: p.db, table: p.table + "-gone", token: token, ttl: p.ttl, now: p.now, log: p.log}
	rec := call(t, gone, http.MethodPost, "/sync", map[string]any{
		"from": "b", "hlc": hlc.Timestamp{WallMS: time.Now().UnixMilli()}, "digest": map[crdt.ICAO]crdt.Version{}})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("a sync while the table can't be read answered %d %s, want 503", rec.Code, rec.Body)
	}
}

// DynamoDB decides each write by comparing ord strings. That has to agree
// with crdt.Compare on every pair, or the table and the edges would settle
// on different winners and never converge. This checks it against the real
// query engine, not just Go's string order, with logical parts of every
// width a clock reaches (a sweep's stamps reach 38).
func TestTheStoreKeepsWhatMergeKeeps(t *testing.T) {
	p := newPeer(t)
	r := rand.New(rand.NewPCG(1, 2))
	small := []float64{-1.5, 0, 0.25, 39}
	logical := []uint32{0, 1, 9, 15, 16, 255, 4096, hlc.MaxLogical}
	gen := func() crdt.Track {
		return crdt.Track{
			Obs: crdt.Observation{Lat: small[r.IntN(4)], Lon: small[r.IntN(4)], AltFt: small[r.IntN(4)],
				ObservedAtMS: int64(r.IntN(3))},
			TS:     hlc.Timestamp{WallMS: 1_700_000_000_000 + int64(r.IntN(2)), Logical: logical[r.IntN(len(logical))]},
			Origin: []string{"a", "a-", "a0", "b"}[r.IntN(4)],
		}
	}
	for i := range 150 {
		id := crdt.ICAO(fmt.Sprintf("%06X", i))
		a, b := gen(), gen()
		for _, tr := range []crdt.Track{a, b} {
			if _, err := p.put(context.Background(), id, tr, "", false); err != nil {
				t.Fatal(err)
			}
		}
		if got, want := stored(t, p)[id], crdt.Merge(a, b); crdt.Compare(got, want) != 0 {
			t.Fatalf("pair %d: the table kept %+v, merge keeps %+v", i, got, want)
		}
	}
}

func TestConcurrentWritesLeaveTheLargestTrack(t *testing.T) {
	p := newPeer(t)
	var wg sync.WaitGroup
	var want crdt.Track
	var mu sync.Mutex
	for i := range 24 {
		tr := track(int64(i*10), fmt.Sprintf("e%d", i))
		mu.Lock()
		want = crdt.Merge(want, tr)
		mu.Unlock()
		wg.Go(func() { push(t, p, map[crdt.ICAO]crdt.Track{"ABCDEF": tr}) })
	}
	wg.Wait()
	if got := stored(t, p)["ABCDEF"]; crdt.Compare(got, want) != 0 {
		t.Fatalf("after racing writes the table holds %+v, want %+v", got, want)
	}
}

func TestEveryPageIsRead(t *testing.T) {
	p := newPeer(t)
	p.pageSize = 3
	tracks := map[crdt.ICAO]crdt.Track{}
	for i := range 20 {
		tracks[crdt.ICAO(fmt.Sprintf("A%05X", i))] = track(int64(i), "b")
	}
	push(t, p, tracks)
	var h struct {
		Hash string `json:"hash"`
		N    int    `json:"n"`
	}
	if err := json.Unmarshal(call(t, p, http.MethodGet, "/hash", nil).Body.Bytes(), &h); err != nil {
		t.Fatal(err)
	}
	if h.N != 20 || h.Hash != crdt.HashOf(tracks) {
		t.Fatalf("read %d aircraft back (hash %s), want 20 (hash %s)", h.N, h.Hash, crdt.HashOf(tracks))
	}
}

// The table is outside the trust boundary: an item that doesn't decode, or
// whose ord doesn't match its track, is skipped rather than served.
func TestABadItemIsSkipped(t *testing.T) {
	p := newPeer(t)
	push(t, p, map[crdt.ICAO]crdt.Track{"AAAAA1": track(1, "b")})
	good, _ := json.Marshal(track(2, "b"))
	for id, item := range map[string]map[string]types.AttributeValue{
		"AAAAA2": {"t": &types.AttributeValueMemberS{Value: "{not json"}},
		"AAAAA3": {"t": &types.AttributeValueMemberS{Value: string(good)}, "ord": &types.AttributeValueMemberS{Value: "zzz"}},
	} {
		item["icao"] = &types.AttributeValueMemberS{Value: id}
		if _, err := p.db.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: &p.table, Item: item}); err != nil {
			t.Fatal(err)
		}
	}
	got, _, bad, err := p.load(context.Background())
	if err != nil || len(bad) != 2 || len(got) != 1 {
		t.Fatalf("load: %d good, bad %v, %v; want 1 and 2", len(got), bad, err)
	}
	if rec := call(t, p, http.MethodGet, "/state", nil); rec.Code != http.StatusOK {
		t.Fatalf("state with bad items in the table: %d", rec.Code)
	}
}

// A bad item still holds its key and ord. A valid write for that aircraft has
// to replace it whatever it holds: a corrupt ord that sorts high, a valid
// track with a mismatched ord, no ord at all, an ord that isn't a string, or
// a track no clock would accept (a stamp a thousand years ahead). Otherwise
// the aircraft is blocked for good.
func TestAValidWriteReplacesABadItem(t *testing.T) {
	p := newPeer(t)
	future := track(1, "b")
	future.TS.WallMS += 1000 * 365 * 24 * time.Hour.Milliseconds()
	futureJSON, _ := json.Marshal(future)
	goodJSON, _ := json.Marshal(track(1, "b"))
	bad := map[crdt.ICAO]map[string]types.AttributeValue{
		"AAAAA1": {"t": &types.AttributeValueMemberS{Value: "{not json"}, "ord": &types.AttributeValueMemberS{Value: "zzzzzzzz"}},
		"AAAAA2": {"t": &types.AttributeValueMemberS{Value: string(goodJSON)}, "ord": &types.AttributeValueMemberS{Value: "zzzzzzzz"}},
		"AAAAA3": {"t": &types.AttributeValueMemberS{Value: "{not json"}},
		"AAAAA4": {"t": &types.AttributeValueMemberS{Value: string(futureJSON)}, "ord": &types.AttributeValueMemberS{Value: crdt.Ord(future)}},
		"AAAAA5": {"t": &types.AttributeValueMemberS{Value: "{not json"}, "ord": &types.AttributeValueMemberN{Value: "1"}},
		"AAAAA6": {"t": &types.AttributeValueMemberS{Value: "{not json"}, "ord": &types.AttributeValueMemberBOOL{Value: false}},
		"AAAAA7": {"t": &types.AttributeValueMemberS{Value: "{not json"}, "ord": &types.AttributeValueMemberNULL{Value: true}},
	}
	valid := map[crdt.ICAO]crdt.Track{}
	for id, item := range bad {
		item["icao"] = &types.AttributeValueMemberS{Value: string(id)}
		if _, err := p.db.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: &p.table, Item: item}); err != nil {
			t.Fatal(err)
		}
		valid[id] = track(2, "c")
	}
	push(t, p, valid)
	if got := stored(t, p); crdt.HashOf(got) != crdt.HashOf(valid) {
		t.Fatalf("after a valid push the table holds %d good tracks of %d", len(got), len(valid))
	}
}

// Restore refuses a stamp past this instance's drift limit. An instance whose
// clock runs behind another's can read a valid track that way, one an edge
// 4 s fast wrote through the other. That track isn't bad, so a smaller one
// must not replace it, or the table's value moves down.
func TestASlowerInstanceNeverMovesTheTableDown(t *testing.T) {
	a := newPeer(t)
	b := &peer{db: a.db, table: a.table, token: a.token, ttl: a.ttl, log: a.log, // the same table,
		now: func() time.Time { return time.Now().Add(-2 * time.Second) }} // a clock 2 s behind a's
	now := time.Now().UnixMilli()
	hi := crdt.Track{Obs: crdt.Observation{Lat: 39, Lon: -77, ObservedAtMS: now}, TS: hlc.Timestamp{WallMS: now + 4000}, Origin: "a"}
	lo := crdt.Track{Obs: crdt.Observation{Lat: 38, Lon: -76, ObservedAtMS: now}, TS: hlc.Timestamp{WallMS: now}, Origin: "c"}
	for _, c := range []struct {
		via  *peer
		from string
		tr   crdt.Track
	}{{a, "a", hi}, {b, "c", lo}} {
		rec := call(t, c.via, http.MethodPost, "/push", map[string]any{
			"from": c.from, "hlc": hlc.Timestamp{WallMS: now - 2000}, "tracks": map[crdt.ICAO]crdt.Track{"ABCDEF": c.tr}})
		if rec.Code != http.StatusNoContent {
			t.Fatalf("push from %s: %d %s", c.from, rec.Code, rec.Body)
		}
	}
	if got := stored(t, a)["ABCDEF"]; crdt.Compare(got, hi) != 0 {
		t.Fatalf("the table held the track stamped %v and now holds the smaller one stamped %v", hi.TS, got.TS)
	}
}

// The table's own TTL attribute is the stamp plus the track TTL, in epoch
// seconds, so DynamoDB drops an aircraft after the edges would.
func TestItemsExpireWithTheirTrack(t *testing.T) {
	p := newPeer(t)
	tr := track(1, "b")
	push(t, p, map[crdt.ICAO]crdt.Track{"AAAAA1": tr})
	out, err := p.db.GetItem(context.Background(), &dynamodb.GetItemInput{TableName: &p.table,
		Key: map[string]types.AttributeValue{"icao": &types.AttributeValueMemberS{Value: "AAAAA1"}}})
	if err != nil {
		t.Fatal(err)
	}
	got := out.Item["expires"].(*types.AttributeValueMemberN).Value
	if want := fmt.Sprint(time.UnixMilli(tr.TS.WallMS).Add(p.ttl).Unix() + 1); got != want {
		t.Fatalf("expires %s, want %s", got, want)
	}
}

// Two edges that can't reach each other, only the cloud, still converge
// through it.
func TestEdgesConvergeThroughTheCloud(t *testing.T) {
	p := newPeer(t)
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	edge := func(id string) *node.Node {
		n, err := node.New(node.Config{ID: id, Peers: []string{srv.URL}, SyncInterval: 50 * time.Millisecond,
			StartupHold: -1, Client: &http.Client{Timeout: 5 * time.Second, Transport: bearer{token}}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go n.Run(ctx)
		return n
	}
	a, b := edge("a"), edge("b")
	a.Ingest("AAAAA1", crdt.Observation{Lat: 39, Lon: -77, ObservedAtMS: 1})
	b.Ingest("BBBBB1", crdt.Observation{Lat: 38, Lon: -76, ObservedAtMS: 2})
	deadline := time.Now().Add(15 * time.Second)
	for a.Hash() != b.Hash() || len(a.Tracks()) != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("edges did not converge through the cloud: a has %d, b has %d", len(a.Tracks()), len(b.Tracks()))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := stored(t, p); crdt.HashOf(got) != a.Hash() {
		t.Fatal("the table does not hold what the edges converged on")
	}
}

// failPut fails every PutItem for one aircraft while on is set, at the
// transport, as a store outage or exhausted throttling retries would.
type failPut struct {
	next interface {
		Do(*http.Request) (*http.Response, error)
	}
	icao string
	on   *atomic.Bool
}

func (f failPut) Do(r *http.Request) (*http.Response, error) {
	if f.on.Load() && r.Header.Get("X-Amz-Target") == "DynamoDB_20120810.PutItem" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if bytes.Contains(body, []byte(`"icao":{"S":"`+f.icao+`"}`)) {
			return nil, fmt.Errorf("injected: the store is unavailable for %s", f.icao)
		}
	}
	return f.next.Do(r)
}

func TestAStoreFailureMidRequestLosesNothing(t *testing.T) {
	p := newPeer(t)
	var failing atomic.Bool
	failing.Store(true)
	opts := p.db.Options()
	opts.HTTPClient = failPut{next: opts.HTTPClient, icao: "AAAAA1", on: &failing}
	opts.Retryer = aws.NopRetryer{} // the SDK's retries are its own business; this is what happens after them
	broken := &peer{db: dynamodb.New(opts), table: p.table, token: token, ttl: p.ttl, now: p.now, log: p.log}

	tracks := map[crdt.ICAO]crdt.Track{"AAAAA1": track(1, "b"), "AAAAA2": track(2, "b"), "AAAAA3": track(3, "b")}
	rec := call(t, broken, http.MethodPost, "/push", map[string]any{
		"from": "b", "hlc": hlc.Timestamp{WallMS: time.Now().UnixMilli()}, "tracks": tracks})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("a request whose write failed answered %d, want 503 so the edge's next round resends", rec.Code)
	}
	got := stored(t, p)
	if _, ok := got["AAAAA1"]; ok || len(got) != 2 {
		t.Fatalf("after one failed write the table holds %d tracks, want the other 2", len(got))
	}

	failing.Store(false)
	push(t, broken, tracks) // the edge's next round sends what is still missing
	if got := stored(t, p); crdt.HashOf(got) != crdt.HashOf(tracks) {
		t.Fatal("once the store came back, the table does not hold what the edge sent")
	}
}

type bearer struct{ token string }

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

func TestAdaptTranslatesTheEvent(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Seen", fmt.Sprintf("%s %s %s %s", r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization")))
		w.WriteHeader(http.StatusTeapot)
		w.Write(body)
	})
	e := events.APIGatewayV2HTTPRequest{RawPath: "/push", RawQueryString: "x=1",
		Headers: map[string]string{"authorization": "Bearer t"}, Body: base64.StdEncoding.EncodeToString([]byte(`{"a":1}`)),
		IsBase64Encoded: true}
	e.RequestContext.HTTP.Method = http.MethodPost
	resp, err := adapt(h)(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusTeapot || resp.Body != `{"a":1}` || resp.Headers["X-Seen"] != "POST /push x=1 Bearer t" {
		t.Fatalf("response %+v", resp)
	}
	e.Body = "%%%"
	if resp, _ := adapt(h)(context.Background(), e); resp.StatusCode != http.StatusBadRequest || strings.Contains(resp.Body, "{") {
		t.Fatalf("bad base64 got %+v", resp)
	}
}
