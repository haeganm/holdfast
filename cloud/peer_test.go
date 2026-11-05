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
	got, bad, err := p.load(context.Background())
	if err != nil || bad != 0 {
		t.Fatalf("load: %v, %d bad items", err, bad)
	}
	return got
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

// DynamoDB decides each write by comparing ord strings. That has to agree
// with crdt.Compare on every pair, or the table and the edges would settle
// on different winners and never converge. This checks it against the real
// query engine, not just Go's string order.
func TestTheStoreKeepsWhatMergeKeeps(t *testing.T) {
	p := newPeer(t)
	r := rand.New(rand.NewPCG(1, 2))
	small := []float64{-1.5, 0, 0.25, 39}
	gen := func() crdt.Track {
		return crdt.Track{
			Obs: crdt.Observation{Lat: small[r.IntN(4)], Lon: small[r.IntN(4)], AltFt: small[r.IntN(4)],
				ObservedAtMS: int64(r.IntN(3))},
			TS:     hlc.Timestamp{WallMS: 1_700_000_000_000 + int64(r.IntN(2)), Logical: uint32(r.IntN(2))},
			Origin: []string{"a", "a-", "a0", "b"}[r.IntN(4)],
		}
	}
	for i := range 150 {
		id := crdt.ICAO(fmt.Sprintf("%06X", i))
		a, b := gen(), gen()
		for _, tr := range []crdt.Track{a, b} {
			if _, err := p.put(context.Background(), id, tr); err != nil {
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
	got, bad, err := p.load(context.Background())
	if err != nil || bad != 2 || len(got) != 1 {
		t.Fatalf("load: %d good, %d bad, %v; want 1 and 2", len(got), bad, err)
	}
	if rec := call(t, p, http.MethodGet, "/state", nil); rec.Code != http.StatusOK {
		t.Fatalf("state with bad items in the table: %d", rec.Code)
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
