// Command cloud is the holdfast cloud peer: an edge node's own merge code,
// run as an AWS Lambda behind an API Gateway HTTP API, with the picture kept
// in DynamoDB between invocations. Edges list it as a peer. It never starts a
// sync itself, so it does nothing, and costs nothing, while nobody calls it.
//
// Every request builds a fresh node from the table, lets the node's own HTTP
// handler answer it, and writes back each track the request changed, on the
// condition that the stored track still sorts below it. Merge is a max, so
// concurrent invocations converge: a write can only replace a smaller track,
// whichever order they land in.
package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/haeganm/holdfast/internal/crdt"
	"github.com/haeganm/holdfast/internal/node"
)

// The table holds one item per aircraft:
//
//	icao     S  key
//	ord      S  crdt.Ord of the track, which DynamoDB can compare
//	t        S  the track as JSON
//	expires  N  epoch seconds after which DynamoDB may delete the item
const writers = 16 // concurrent PutItems per request

type peer struct {
	db       *dynamodb.Client
	table    string
	token    string // compared in constant time; empty only for local runs
	ttl      time.Duration
	now      func() time.Time
	log      *slog.Logger
	pageSize int32 // 0 for DynamoDB's own; tests shrink it to exercise paging
	stats    stats
}

// stats count this process's work for edges (their POST /sync and /push;
// reads such as the harness's /state polls are left out), served at
// GET /peer. In Lambda that is one instance; under the harness it is the
// whole run, which turns into a measured cost per report before anything is
// deployed.
type stats struct {
	Requests  atomic.Int64 `json:"requests"`
	ItemsRead atomic.Int64 `json:"items_read"`
	Writes    atomic.Int64 `json:"writes"`
	Won       atomic.Int64 `json:"won"`
	Failed    atomic.Int64 `json:"failed"` // requests answered 503
}

func (s *stats) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]int64{
		"requests": s.Requests.Load(), "items_read": s.ItemsRead.Load(),
		"writes": s.Writes.Load(), "won": s.Won.Load(), "failed": s.Failed.Load(),
	})
}

func (p *peer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p.token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+p.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/peer" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&p.stats)
		return
	}
	edge := r.Method == http.MethodPost
	if edge {
		p.stats.Requests.Add(1)
	}
	ctx := r.Context()
	stored, ords, bad, err := p.load(ctx)
	if err != nil {
		if edge {
			p.stats.Failed.Add(1)
		}
		p.log.Error("load", "err", err)
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	if edge {
		p.stats.ItemsRead.Add(int64(len(stored) + len(bad)))
	}
	n := newNode(node.Config{ID: "cloud", Now: p.now, StartupHold: -1, TrackTTL: p.ttl})
	// An item that doesn't decode, or that no clock would accept, still holds
	// its key and its ord in the table. A valid write must be able to replace
	// it, or a corrupt item that sorts high would block that aircraft for good.
	bad = append(bad, corrupt(stored, n.Restore(stored))...)
	replace := make(map[crdt.ICAO]string, len(bad))
	for _, id := range bad {
		replace[id] = ords[id]
	}
	before := n.Tracks()
	resp := &buffered{header: http.Header{}, status: http.StatusOK}
	n.Handler().ServeHTTP(resp, r)

	changed := map[crdt.ICAO]crdt.Track{}
	for id, t := range n.Tracks() {
		if b, ok := before[id]; !ok || crdt.Compare(t, b) != 0 {
			changed[id] = t
		}
	}
	won, err := p.save(ctx, changed, replace)
	p.stats.Writes.Add(int64(len(changed)))
	p.stats.Won.Add(int64(won))
	p.log.Info("request", "path", r.URL.Path, "status", resp.status, "stored", len(stored), "bad", len(bad),
		"changed", len(changed), "won", won, "err", err)
	if err != nil {
		// Nothing is lost: what was written is a max-merge, and the edge's
		// next round sends whatever is still missing.
		p.stats.Failed.Add(1)
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	for k, v := range resp.header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.status)
	w.Write(resp.body.Bytes())
}

// corrupt returns those of the refused aircraft whose stored track no clock
// would accept. Restore also refuses a valid track only because its stamp is
// past this instance's drift limit, as one written through an instance whose
// clock reads ahead of this one's can be. That item isn't corrupt: the
// ordinary condition decides it, or a slower instance would replace it with
// a smaller track and move the table's value down. What a node with no drift
// limit to speak of (292 years) and no expiry still refuses, time can't fix.
func corrupt(stored map[crdt.ICAO]crdt.Track, refused []crdt.ICAO) []crdt.ICAO {
	if len(refused) == 0 {
		return nil
	}
	again := make(map[crdt.ICAO]crdt.Track, len(refused))
	for _, id := range refused {
		again[id] = stored[id]
	}
	return newNode(node.Config{ID: "cloud", MaxDrift: math.MaxInt64, StartupHold: -1, TrackTTL: -1}).Restore(again)
}

// newNode builds a node from a constant config, which can't fail.
func newNode(cfg node.Config) *node.Node {
	n, err := node.New(cfg)
	if err != nil {
		panic(err)
	}
	return n
}

// load reads the whole table. An item that does not decode, or whose ord
// does not match its track, is skipped and reported: the table is outside the
// node's trust boundary, and Restore validates what does decode. ords holds
// every item's stored ord as it is, so a bad item can be replaced exactly.
func (p *peer) load(ctx context.Context) (tracks map[crdt.ICAO]crdt.Track, ords map[crdt.ICAO]string, bad []crdt.ICAO, err error) {
	in := &dynamodb.ScanInput{TableName: &p.table, ConsistentRead: aws.Bool(true)}
	if p.pageSize > 0 {
		in.Limit = &p.pageSize
	}
	tracks, ords = map[crdt.ICAO]crdt.Track{}, map[crdt.ICAO]string{}
	for pages := dynamodb.NewScanPaginator(p.db, in); pages.HasMorePages(); {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, item := range page.Items {
			id := crdt.ICAO(attr(item, "icao"))
			ords[id] = attr(item, "ord")
			t, err := decode(item)
			if err != nil {
				bad = append(bad, id)
				continue
			}
			tracks[id] = t
		}
	}
	return tracks, ords, bad, nil
}

// attr is a string attribute's value, or "" if it is missing or not a string.
func attr(item map[string]types.AttributeValue, k string) string {
	if v, ok := item[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

func decode(item map[string]types.AttributeValue) (crdt.Track, error) {
	var t crdt.Track
	dec := json.NewDecoder(bytes.NewReader([]byte(attr(item, "t"))))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&t); err != nil {
		return t, fmt.Errorf("track: %w", err)
	}
	// Anything after the track is refused, as a node refuses it on the wire.
	if _, err := dec.Token(); err != io.EOF {
		return t, errors.New("track: unexpected data after it")
	}
	if attr(item, "ord") != crdt.Ord(t) {
		return t, errors.New("ord does not match the track")
	}
	return t, nil
}

// save writes each changed track unless a greater one is already stored, and
// returns how many writes won. Losing the condition is a lost merge, not an
// error, including when the SDK retries a write that had in fact landed.
// replace maps the aircraft whose stored item was bad to that item's ord.
func (p *peer) save(ctx context.Context, tracks map[crdt.ICAO]crdt.Track, replace map[crdt.ICAO]string) (int, error) {
	type job struct {
		id crdt.ICAO
		t  crdt.Track
	}
	jobs := make(chan job)
	var mu sync.Mutex
	won := 0
	var errs []error
	var wg sync.WaitGroup
	for range min(writers, len(tracks)) {
		wg.Go(func() {
			for j := range jobs {
				badOrd, bad := replace[j.id]
				ok, err := p.put(ctx, j.id, j.t, badOrd, bad)
				mu.Lock()
				if ok {
					won++
				}
				if err != nil {
					errs = append(errs, err)
				}
				mu.Unlock()
			}
		})
	}
	for id, t := range tracks {
		jobs <- job{id, t}
	}
	close(jobs)
	wg.Wait()
	return won, errors.Join(errs...)
}

// put writes t if the stored track sorts below it. When this request found
// the stored item bad, it may also replace exactly that item: by its ord if
// that was a string, or because it has no string ord (none, or one of another
// type, such as a number, which no string compares with). If another
// invocation replaced it in the meantime, the ordinary condition decides.
func (p *peer) put(ctx context.Context, id crdt.ICAO, t crdt.Track, badOrd string, bad bool) (bool, error) {
	body, err := json.Marshal(t)
	if err != nil {
		return false, err
	}
	ord := crdt.Ord(t)
	expires := time.UnixMilli(t.TS.WallMS).Add(p.ttl).Unix() + 1
	cond := "attribute_not_exists(icao) OR ord < :ord"
	values := map[string]types.AttributeValue{":ord": &types.AttributeValueMemberS{Value: ord}}
	switch {
	case bad && badOrd != "":
		cond += " OR ord = :bad"
		values[":bad"] = &types.AttributeValueMemberS{Value: badOrd}
	case bad:
		cond += " OR NOT attribute_type(ord, :s)"
		values[":s"] = &types.AttributeValueMemberS{Value: "S"}
	}
	_, err = p.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.table,
		Item: map[string]types.AttributeValue{
			"icao":    &types.AttributeValueMemberS{Value: string(id)},
			"ord":     &types.AttributeValueMemberS{Value: ord},
			"t":       &types.AttributeValueMemberS{Value: string(body)},
			"expires": &types.AttributeValueMemberN{Value: strconv.FormatInt(expires, 10)},
		},
		ConditionExpression:       aws.String(cond),
		ExpressionAttributeValues: values,
	})
	var lost *types.ConditionalCheckFailedException
	if errors.As(err, &lost) {
		return false, nil
	}
	return err == nil, err
}

// buffered holds a response until the writes behind it have landed, so a
// caller never sees a success for something that was not stored.
type buffered struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (b *buffered) Header() http.Header         { return b.header }
func (b *buffered) Write(p []byte) (int, error) { return b.body.Write(p) }
func (b *buffered) WriteHeader(status int)      { b.status = status }
