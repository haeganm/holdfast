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
	"log/slog"
	"net/http"
	"strconv"
	"sync"
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
}

func (p *peer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p.token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+p.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	ctx := r.Context()
	stored, bad, err := p.load(ctx)
	if err != nil {
		p.log.Error("load", "err", err)
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	n, err := node.New(node.Config{ID: "cloud", Now: p.now, StartupHold: -1, TrackTTL: p.ttl})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bad += n.Restore(stored)
	before := n.Tracks()
	resp := &buffered{header: http.Header{}, status: http.StatusOK}
	n.Handler().ServeHTTP(resp, r)

	changed := map[crdt.ICAO]crdt.Track{}
	for id, t := range n.Tracks() {
		if b, ok := before[id]; !ok || crdt.Compare(t, b) != 0 {
			changed[id] = t
		}
	}
	won, err := p.save(ctx, changed)
	p.log.Info("request", "path", r.URL.Path, "status", resp.status, "stored", len(stored), "bad", bad,
		"changed", len(changed), "won", won, "err", err)
	if err != nil {
		// Nothing is lost: what was written is a max-merge, and the edge's
		// next round sends whatever is still missing.
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	for k, v := range resp.header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.status)
	w.Write(resp.body.Bytes())
}

// load reads the whole table. An item that does not decode, or whose ord
// does not match its track, is skipped and counted: the table is outside the
// node's trust boundary, and Restore validates what does decode.
func (p *peer) load(ctx context.Context) (map[crdt.ICAO]crdt.Track, int, error) {
	in := &dynamodb.ScanInput{TableName: &p.table, ConsistentRead: aws.Bool(true)}
	if p.pageSize > 0 {
		in.Limit = &p.pageSize
	}
	out := map[crdt.ICAO]crdt.Track{}
	bad := 0
	for pages := dynamodb.NewScanPaginator(p.db, in); pages.HasMorePages(); {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, 0, err
		}
		for _, item := range page.Items {
			id, t, err := decode(item)
			if err != nil {
				bad++
				continue
			}
			out[id] = t
		}
	}
	return out, bad, nil
}

func decode(item map[string]types.AttributeValue) (crdt.ICAO, crdt.Track, error) {
	str := func(k string) string {
		if v, ok := item[k].(*types.AttributeValueMemberS); ok {
			return v.Value
		}
		return ""
	}
	var t crdt.Track
	dec := json.NewDecoder(bytes.NewReader([]byte(str("t"))))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&t); err != nil {
		return "", t, fmt.Errorf("track: %w", err)
	}
	if str("ord") != crdt.Ord(t) {
		return "", t, errors.New("ord does not match the track")
	}
	return crdt.ICAO(str("icao")), t, nil
}

// save writes each changed track unless a greater one is already stored, and
// returns how many writes won. Losing the condition is a lost merge, not an
// error, including when the SDK retries a write that had in fact landed.
func (p *peer) save(ctx context.Context, tracks map[crdt.ICAO]crdt.Track) (int, error) {
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
				ok, err := p.put(ctx, j.id, j.t)
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

func (p *peer) put(ctx context.Context, id crdt.ICAO, t crdt.Track) (bool, error) {
	body, err := json.Marshal(t)
	if err != nil {
		return false, err
	}
	ord := crdt.Ord(t)
	expires := time.UnixMilli(t.TS.WallMS).Add(p.ttl).Unix() + 1
	_, err = p.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.table,
		Item: map[string]types.AttributeValue{
			"icao":    &types.AttributeValueMemberS{Value: string(id)},
			"ord":     &types.AttributeValueMemberS{Value: ord},
			"t":       &types.AttributeValueMemberS{Value: string(body)},
			"expires": &types.AttributeValueMemberN{Value: strconv.FormatInt(expires, 10)},
		},
		ConditionExpression:       aws.String("attribute_not_exists(icao) OR ord < :ord"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":ord": &types.AttributeValueMemberS{Value: ord}},
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
