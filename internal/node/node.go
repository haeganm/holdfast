// Package node runs one edge node: it stamps local observations with its
// hybrid logical clock, keeps a replica of the fused picture, and syncs with
// peers by anti-entropy over HTTP.
//
// Every exchange is idempotent, so a lost request is never retried: the next
// tick is the retry. That is what lets the protocol run over a link that drops
// half its packets without any extra machinery.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"math/rand/v2"
	"net/http"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/haeganm/holdfast/internal/crdt"
	"github.com/haeganm/holdfast/internal/hlc"
)

const (
	maxBody = 4 << 20 // bytes per request or response body

	// ponytail: the replica holds at most maxEntries aircraft, so a digest
	// of all of it always fits in one message. Once it is full, a track for
	// a new aircraft is refused until expiry makes room, so a busy sky, or a
	// peer pushing made-up aircraft, keeps new ones out. Paging the digest,
	// or Merkle ranges, is the upgrade when the picture outgrows it.
	maxEntries = 10_000 // aircraft per digest, track batch or replica

)

var (
	icaoRe   = regexp.MustCompile(`^[0-9A-F]{6}$`)
	originRe = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
)

// Config describes a node. Zero values for SyncInterval, MaxDrift, Client,
// Now and Logger get the defaults noted on each field.
type Config struct {
	ID           string        // short lowercase name, also the Origin of its writes
	Peers        []string      // base URLs, such as http://172.30.0.12:8080
	SyncInterval time.Duration // per peer; default 1s
	Wall         bool          // stamp with the plain wall clock (the baseline) instead of the HLC

	// ClockOffset skews the physical clock that drives the HLC, to simulate a
	// node whose clock is wrong. Measurement always uses true time.
	ClockOffset time.Duration

	// MaxDrift bounds how far ahead of this node's physical clock a remote
	// stamp may be. Without it one peer with a broken clock drags every HLC
	// forward and wins every merge. A message with any stamp past the bound
	// is rejected whole. Since HLCs drag each other forward, convergence
	// needs the spread between the fastest and slowest physical clock in the
	// system to stay under MaxDrift. Default 5s.
	MaxDrift time.Duration

	// StartupHold is how long a newly started node holds its own reports
	// before stamping them. A restarted node's clock knows nothing of the
	// stamps its previous run issued, and can be behind them; any stamp in
	// the system is at most the clock spread ahead of this node's physical
	// clock, and that spread is assumed to stay under MaxDrift. So after
	// holding for MaxDrift, every stamp this node issues sorts after every
	// stamp that existed when it started, its own old ones included. Held
	// reports are kept, the latest per aircraft, and stamped when the hold
	// ends. Default MaxDrift; negative for none, which is safe only when no
	// stamp can be ahead of this node's clock. The wall-clock baseline never
	// holds: it makes no ordering promise to keep.
	StartupHold time.Duration

	// TrackTTL drops an aircraft whose stored stamp is older than this, so
	// a live picture doesn't grow forever. A node also refuses an incoming
	// track it has already expired and never asks a peer for one, so an
	// expired version can't come back. Replicas agree on what has expired to
	// within their clock spread. Default 5 minutes; negative keeps every
	// track forever.
	TrackTTL time.Duration

	Client *http.Client     // default: 2s timeout
	Now    func() time.Time // true time; default time.Now
	Logger *slog.Logger     // default: discard
}

// Counters are cumulative since start.
type Counters struct {
	IngestTotal int64 `json:"ingest_total"`
	IngestLost  int64 `json:"ingest_lost"` // a local observation lost to an older one already stored
	IngestHeld  int64 `json:"ingest_held"` // a local observation arrived during the startup hold
	// IngestRegress: a local observation replaced a stored one that was
	// observed later. Last-writer-wins orders writes, not observations, so a
	// sensor that hears an old position after a peer delivered a newer one
	// moves the picture back. Synthetic traces can't cause it; live feeds can.
	IngestRegress int64 `json:"ingest_regress"`
	SensorErrors  int64 `json:"sensor_errors"` // failed polls of this node's live feed
	MergeApplied  int64 `json:"merge_applied"` // a received track won its merge
	MergeStale    int64 `json:"merge_stale"`   // a received track lost or was a redelivery
	MergeRegress  int64 `json:"merge_regress"` // a received track won but was older than what it replaced
	MergeExpired  int64 `json:"merge_expired"` // a received track had already expired here
	Expired       int64 `json:"expired"`       // aircraft dropped by TrackTTL
	Capped        int64 `json:"capped"`        // a track for a new aircraft refused because the replica was full
	SyncOK        int64 `json:"sync_ok"`
	SyncErr       int64 `json:"sync_err"`
	Rejected      int64 `json:"rejected"`  // inbound messages refused at the trust boundary
	Aborted       int64 `json:"aborted"`   // inbound requests cut off before the whole body arrived
	BytesOut      int64 `json:"bytes_out"` // request bodies this node sent as initiator, for answered requests
	BytesIn       int64 `json:"bytes_in"`  // response bodies this node received as initiator
}

type counters struct {
	ingestTotal, ingestLost, ingestHeld    atomic.Int64
	ingestRegress, sensorErrors            atomic.Int64
	mergeApplied, mergeStale, mergeRegress atomic.Int64
	mergeExpired, expired, capped          atomic.Int64
	syncOK, syncErr, rejected, aborted     atomic.Int64
	bytesOut, bytesIn                      atomic.Int64
}

// Node is one edge node.
type Node struct {
	id       string
	peers    []string
	interval time.Duration
	maxDrift time.Duration
	ttl      time.Duration
	cut      atomic.Int64 // the highest expiry cutoff so far; see cutoff
	trueNow  func() time.Time
	offset   time.Duration // the physical clock's error, Config.ClockOffset; see physNow
	clock    *hlc.Clock
	tracks   *crdt.Map
	client   *http.Client
	log      *slog.Logger
	c        counters

	// mu serializes the two write paths, stamp-then-put and
	// advance-clock-then-merge. Together they keep the invariant that every
	// stamp in the replica is at or below the clock, so a local write under
	// the HLC always sorts after everything this node has already seen.
	mu  sync.Mutex
	own map[crdt.ICAO]int64 // latest ObservedAtMS this node ingested itself, per aircraft

	// held is non-nil during the startup hold, which lasts until the
	// physical clock reads holdUntilMS. Both are guarded by mu.
	held        map[crdt.ICAO]crdt.Observation
	holdUntilMS int64
}

func New(cfg Config) (*Node, error) {
	if !originRe.MatchString(cfg.ID) {
		return nil, fmt.Errorf("node id %q: want 1-32 of [a-z0-9-]", cfg.ID)
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = time.Second
	}
	if cfg.MaxDrift == 0 {
		cfg.MaxDrift = 5 * time.Second
	}
	if cfg.StartupHold == 0 {
		cfg.StartupHold = cfg.MaxDrift
	}
	if cfg.TrackTTL == 0 {
		cfg.TrackTTL = 5 * time.Minute
	}
	if cfg.SyncInterval < 0 || cfg.MaxDrift < 0 {
		return nil, errors.New("sync interval and max drift must be positive")
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 2 * time.Second}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	n := &Node{
		id:       cfg.ID,
		peers:    cfg.Peers,
		interval: cfg.SyncInterval,
		maxDrift: cfg.MaxDrift,
		ttl:      cfg.TrackTTL,
		trueNow:  cfg.Now,
		offset:   cfg.ClockOffset,
		tracks:   crdt.NewMap(),
		client:   cfg.Client,
		log:      cfg.Logger.With("node", cfg.ID),
		own:      map[crdt.ICAO]int64{},
	}
	n.clock = hlc.New(n.physNow, cfg.Wall)
	n.cut.Store(math.MinInt64)
	if cfg.StartupHold > 0 && !cfg.Wall {
		n.held = map[crdt.ICAO]crdt.Observation{}
		n.holdUntilMS = n.physNow().Add(cfg.StartupHold).UnixMilli()
	}
	return n, nil
}

// physNow is this node's own physical clock, the one that drives its HLC:
// true time plus the clock's error.
func (n *Node) physNow() time.Time { return n.trueNow().Add(n.offset) }

// Ingest stamps a local observation and merges it into the replica, or keeps
// it until the startup hold ends.
func (n *Node) Ingest(id crdt.ICAO, obs crdt.Observation) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.c.ingestTotal.Add(1)
	if obs.ObservedAtMS > n.own[id] {
		n.own[id] = obs.ObservedAtMS
	}
	if n.holding() {
		if prev, ok := n.held[id]; !ok || obs.ObservedAtMS >= prev.ObservedAtMS {
			n.held[id] = obs
		}
		n.c.ingestHeld.Add(1)
		return
	}
	n.stamp(id, obs)
}

// holding reports whether the startup hold is still on, and stamps what it
// held once it is over. Callers hold mu.
func (n *Node) holding() bool {
	if n.held == nil {
		return false
	}
	if n.physNow().UnixMilli() < n.holdUntilMS {
		return true
	}
	held := n.held
	n.held = nil
	ids := slices.Sorted(maps.Keys(held))
	stored := n.tracks.Get(ids)
	for _, id := range ids {
		// The hold made this report late. Stamped over a fresher observation a
		// peer delivered meanwhile, it would win everywhere and move the
		// picture back.
		if cur, ok := stored[id]; ok && cur.Obs.ObservedAtMS > held[id].ObservedAtMS {
			continue
		}
		n.stamp(id, held[id])
	}
	return false
}

// stamp writes a local observation with a fresh stamp. Callers hold mu, so no
// merge can land between taking the stamp and storing it.
func (n *Node) stamp(id crdt.ICAO, obs crdt.Observation) {
	if n.full(id) {
		n.c.capped.Add(1)
		delete(n.own, id) // own keeps only aircraft the replica holds, so it stays bounded too
		return
	}
	won, prev := n.tracks.Put(id, crdt.Track{Obs: obs, TS: n.clock.Now(), Origin: n.id})
	switch {
	case !won && prev.Obs.ObservedAtMS < obs.ObservedAtMS:
		n.c.ingestLost.Add(1)
	case won && prev.Origin != "" && obs.ObservedAtMS < prev.Obs.ObservedAtMS:
		n.c.ingestRegress.Add(1)
	}
}

// full reports whether a track for id would add an aircraft to a replica that
// already holds maxEntries. Callers hold mu, which every write to the replica
// takes, so the answer still holds when they put.
func (n *Node) full(id crdt.ICAO) bool {
	if n.tracks.Len() < maxEntries {
		return false
	}
	_, held := n.tracks.Get([]crdt.ICAO{id})[id]
	return !held
}

// cutoff is the wall time before which a stored stamp has expired. It only
// moves forward: a physical clock can step back (an NTP correction does), and
// a cutoff that followed it would let a peer hand back a version this node
// already dropped.
func (n *Node) cutoff() int64 {
	if n.ttl < 0 {
		return math.MinInt64
	}
	c := n.physNow().Add(-n.ttl).UnixMilli()
	for {
		old := n.cut.Load()
		if c <= old || n.cut.CompareAndSwap(old, c) {
			return max(c, old)
		}
	}
}

// expire drops the aircraft whose tracks have expired.
func (n *Node) expire() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.expireLocked()
}

// expireLocked is expire for a caller that holds mu.
func (n *Node) expireLocked() {
	for _, id := range n.tracks.Expire(n.cutoff()) {
		delete(n.own, id)
		n.c.expired.Add(1)
	}
}

// Restore loads tracks kept outside the node, such as the cloud peer's table,
// and advances the clock past every one it keeps. Each passes the same checks
// as a track in a peer's message, and one that has already expired, or is for
// a new aircraft once the replica is full, is left out, as a peer's would be.
// It returns the aircraft it refused as invalid, so the host can repair what
// it stores for them.
func (n *Node) Restore(tracks map[crdt.ICAO]crdt.Track) (refused []crdt.ICAO) {
	limit := n.physNow().Add(n.maxDrift).UnixMilli()
	keep := make(map[crdt.ICAO]crdt.Track, len(tracks))
	high := hlc.Timestamp{}
	for id, t := range tracks {
		if checkTrack(id, t, limit) != nil {
			refused = append(refused, id)
			continue
		}
		keep[id] = t
		if t.TS.Compare(high) > 0 {
			high = t.TS
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	cut := n.cutoff()
	maps.DeleteFunc(keep, func(_ crdt.ICAO, t crdt.Track) bool { return t.TS.WallMS < cut })
	n.clock.Update(high)
	for id, t := range keep {
		if n.full(id) {
			n.c.capped.Add(1)
			continue
		}
		n.tracks.Put(id, t)
	}
	slices.Sort(refused)
	return refused
}

// Tracks returns a copy of the replica.
func (n *Node) Tracks() map[crdt.ICAO]crdt.Track { return n.tracks.Snapshot() }

// SensorFailed counts a failed poll of this node's live feed.
func (n *Node) SensorFailed() { n.c.sensorErrors.Add(1) }

// receive validates a message from a peer, advances the clock past every
// stamp in it, then merges its tracks. The clock moves first, under the same
// lock as the merge, so no local write can be stamped below a merged track.
func (n *Node) receive(msgTS hlc.Timestamp, tracks map[crdt.ICAO]crdt.Track) error {
	if err := n.validate(msgTS, tracks); err != nil {
		n.c.rejected.Add(1)
		return err
	}
	high := msgTS
	for _, t := range tracks {
		if t.TS.Compare(high) > 0 {
			high = t.TS
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	// The cutoff is read under the lock, like expire's: read before it, an
	// expire could drop a version in between and this merge bring it back.
	cut := n.cutoff()
	n.clock.Update(high)
	for id, t := range tracks {
		if t.TS.WallMS < cut {
			n.c.mergeExpired.Add(1)
			continue
		}
		if n.full(id) {
			n.c.capped.Add(1)
			continue
		}
		won, prev := n.tracks.Put(id, t)
		switch {
		case !won:
			n.c.mergeStale.Add(1)
		case prev.Origin != "" && t.Obs.ObservedAtMS < prev.Obs.ObservedAtMS:
			n.c.mergeApplied.Add(1)
			n.c.mergeRegress.Add(1)
		default:
			n.c.mergeApplied.Add(1)
		}
	}
	return nil
}

// validate is the trust boundary for anything a peer sends.
func (n *Node) validate(msgTS hlc.Timestamp, tracks map[crdt.ICAO]crdt.Track) error {
	limit := n.physNow().Add(n.maxDrift).UnixMilli()
	if err := checkStamp(msgTS, limit); err != nil {
		return fmt.Errorf("message stamp: %w", err)
	}
	if len(tracks) > maxEntries {
		return fmt.Errorf("%d tracks in one message, limit %d", len(tracks), maxEntries)
	}
	for id, t := range tracks {
		if err := checkTrack(id, t, limit); err != nil {
			return fmt.Errorf("track %q: %w", id, err)
		}
	}
	return nil
}

func checkStamp(ts hlc.Timestamp, limit int64) error {
	if ts.WallMS > limit {
		return fmt.Errorf("stamp %v is more than the allowed drift ahead of this node's clock (limit %d)", ts, limit)
	}
	if ts.WallMS < 0 {
		return fmt.Errorf("negative stamp %v", ts)
	}
	// A logical part near MaxUint32 would wrap the receiving clock to zero,
	// below the track it just stored. Clocks never produce one over the bound.
	if ts.Logical > hlc.MaxLogical {
		return fmt.Errorf("stamp %v has a logical part over %d", ts, hlc.MaxLogical)
	}
	return nil
}

func checkTrack(id crdt.ICAO, t crdt.Track, limit int64) error {
	if !icaoRe.MatchString(string(id)) {
		return errors.New("aircraft id must be six uppercase hex digits")
	}
	if !originRe.MatchString(t.Origin) {
		return fmt.Errorf("origin %q: want 1-32 of [a-z0-9-]", t.Origin)
	}
	if err := checkStamp(t.TS, limit); err != nil {
		return err
	}
	o := t.Obs
	for _, f := range []float64{o.Lat, o.Lon, o.AltFt, o.GSKt, o.HeadingDeg} {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return errors.New("non-finite number in observation")
		}
	}
	if o.Lat < -90 || o.Lat > 90 || o.Lon < -180 || o.Lon > 180 {
		return fmt.Errorf("position %v,%v out of range", o.Lat, o.Lon)
	}
	return nil
}

func checkDigest(d map[crdt.ICAO]crdt.Version) error {
	if len(d) > maxEntries {
		return fmt.Errorf("%d digest entries, limit %d", len(d), maxEntries)
	}
	for id, v := range d {
		if !icaoRe.MatchString(string(id)) {
			return fmt.Errorf("digest aircraft id %q must be six uppercase hex digits", id)
		}
		if !originRe.MatchString(v.Origin) {
			return fmt.Errorf("digest origin %q for %s: want 1-32 of [a-z0-9-]", v.Origin, id)
		}
	}
	return nil
}

// Run syncs with every peer, one loop per peer so a dead peer's timeouts
// never slow the others, until ctx is done.
func (n *Node) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, p := range n.peers {
		wg.Go(func() { n.syncLoop(ctx, p) })
	}
	// Expiry and the end of the startup hold also ride on sync rounds, but
	// a node with no peers has none, and must still forget old aircraft.
	wg.Go(func() {
		tick := time.NewTicker(n.interval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			n.mu.Lock()
			n.holding()
			n.mu.Unlock()
			n.expire()
		}
	})
	wg.Wait()
}

func (n *Node) syncLoop(ctx context.Context, peer string) {
	timer := time.NewTimer(rand.N(n.interval)) // spread the first sync so loops don't run in lockstep
	defer timer.Stop()
	failing := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		err := n.syncOnce(ctx, peer)
		switch {
		case err != nil:
			n.c.syncErr.Add(1)
			if !failing && ctx.Err() == nil {
				n.log.Warn("peer unreachable", "peer", peer, "err", err)
			}
			failing = true
		default:
			n.c.syncOK.Add(1)
			if failing {
				n.log.Info("peer reachable again", "peer", peer)
			}
			failing = false
		}
		timer.Reset(n.interval)
	}
}

type syncRequest struct {
	From   string                     `json:"from"`
	HLC    hlc.Timestamp              `json:"hlc"`
	Digest map[crdt.ICAO]crdt.Version `json:"digest"`
}

type syncResponse struct {
	HLC    hlc.Timestamp            `json:"hlc"`
	Tracks map[crdt.ICAO]crdt.Track `json:"tracks"`
	Want   []crdt.ICAO              `json:"want"`
}

type pushRequest struct {
	From   string                   `json:"from"`
	HLC    hlc.Timestamp            `json:"hlc"`
	Tracks map[crdt.ICAO]crdt.Track `json:"tracks"`
}

// syncOnce is one anti-entropy round: send my digest, merge what the peer
// says I'm missing, then send what the peer says it's missing.
func (n *Node) syncOnce(ctx context.Context, peer string) error {
	n.mu.Lock()
	n.holding() // the hold ends on a tick even if no new report arrives
	n.mu.Unlock()
	n.expire()
	req := syncRequest{From: n.id, HLC: n.clock.Now(), Digest: n.tracks.Digest()}
	var resp syncResponse
	if err := n.post(ctx, peer+"/sync", req, &resp); err != nil {
		return err
	}
	// Checked before anything merges, so a refused response changes nothing.
	if len(resp.Want) > maxEntries {
		n.c.rejected.Add(1)
		return fmt.Errorf("%s/sync asked for %d tracks, limit %d", peer, len(resp.Want), maxEntries)
	}
	if err := n.receive(resp.HLC, resp.Tracks); err != nil {
		return fmt.Errorf("%s/sync response: %w", peer, err)
	}
	if len(resp.Want) == 0 {
		return nil
	}
	push := pushRequest{From: n.id, HLC: n.clock.Now(), Tracks: n.tracks.Get(resp.Want)}
	return n.post(ctx, peer+"/push", push, nil)
}

func (n *Node) post(ctx context.Context, url string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("encode request for %s: %w", url, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Counted once the peer has answered, so attempts that never got through
	// don't count. The harness measures true wire bytes separately.
	n.c.bytesOut.Add(int64(len(body)))
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	n.c.bytesIn.Add(int64(len(data)))
	if err != nil {
		// Cut off on the way, by a timeout or a dropped link: nothing judged
		// it, so like a cut-off request it is not a rejection.
		return fmt.Errorf("read response from %s: %w", url, err)
	}
	// The whole body arrived, so a response refused for its size or contents
	// is a bad message, counted as a request's would be. A status that isn't
	// 2xx is the peer refusing this node, not the other way round.
	if len(data) > maxBody {
		n.c.rejected.Add(1)
		return fmt.Errorf("response from %s exceeds %d bytes", url, maxBody)
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s: %s: %s", url, resp.Status, bytes.TrimSpace(data[:min(len(data), 200)]))
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		n.c.rejected.Add(1)
		return fmt.Errorf("decode response from %s: %w", url, err)
	}
	return nil
}

// Handler serves the sync protocol and the observation endpoints.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sync", n.handleSync)
	mux.HandleFunc("POST /push", n.handlePush)
	mux.HandleFunc("GET /hash", n.handleHash)
	mux.HandleFunc("GET /state", n.handleState)
	mux.HandleFunc("GET /metrics", n.handleMetrics)
	return mux
}

func (n *Node) handleSync(w http.ResponseWriter, r *http.Request) {
	var req syncRequest
	if !n.decode(w, r, &req) {
		return
	}
	if err := checkDigest(req.Digest); err != nil {
		n.c.rejected.Add(1)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := n.receive(req.HLC, nil); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.expire()
	// Never ask for a version this node has already expired: it would only
	// be refused on arrival.
	cut := n.cutoff()
	maps.DeleteFunc(req.Digest, func(_ crdt.ICAO, v crdt.Version) bool { return v.TS.WallMS < cut })
	newer, want := n.tracks.Diff(req.Digest)
	writeJSON(w, syncResponse{HLC: n.clock.Now(), Tracks: newer, Want: want})
}

func (n *Node) handlePush(w http.ResponseWriter, r *http.Request) {
	var req pushRequest
	if !n.decode(w, r, &req) {
		return
	}
	if err := n.receive(req.HLC, req.Tracks); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// decode reads one JSON message with a size limit, rejecting unknown fields
// and a sender id that could not be a node. A body the connection cut off
// (the sender timed out, or the link dropped it) is counted as aborted, not
// rejected: nothing judged it, it just never arrived.
func (n *Node) decode(w http.ResponseWriter, r *http.Request, v interface{ from() string }) bool {
	body := &readErr{ReadCloser: r.Body}
	r.Body = http.MaxBytesReader(w, body, maxBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(v)
	if err == nil && !originRe.MatchString(v.from()) {
		err = fmt.Errorf("sender id %q: want 1-32 of [a-z0-9-]", v.from())
	}
	if err == nil {
		return true
	}
	if body.err != nil {
		n.c.aborted.Add(1)
	} else {
		n.c.rejected.Add(1)
	}
	status := http.StatusBadRequest
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		status = http.StatusRequestEntityTooLarge
	}
	http.Error(w, fmt.Sprintf("%s: %v", r.URL.Path, err), status)
	return false
}

// readErr keeps the first error the transport returned while the body was
// read. A complete body that is bad JSON can also end in "unexpected EOF",
// so the decode error alone can't tell a cut-off request from a malformed one.
type readErr struct {
	io.ReadCloser
	err error
}

func (r *readErr) Read(p []byte) (int, error) {
	k, err := r.ReadCloser.Read(p)
	if err != nil && err != io.EOF && r.err == nil {
		r.err = err
	}
	return k, err
}

func (r *syncRequest) from() string { return r.From }
func (r *pushRequest) from() string { return r.From }

// Hash is the hash of the current replica.
func (n *Node) Hash() string { return n.tracks.Hash() }

func (n *Node) handleHash(w http.ResponseWriter, _ *http.Request) {
	snap := n.tracks.Snapshot()
	writeJSON(w, map[string]any{"hash": crdt.HashOf(snap), "n": len(snap)})
}

// State is what the experiment harness polls.
type State struct {
	ID       string                   `json:"id"`
	NowMS    int64                    `json:"now_ms"` // true time, not the skewed clock
	Hash     string                   `json:"hash"`
	Counters Counters                 `json:"counters"`
	Tracks   map[crdt.ICAO]StateTrack `json:"tracks"`
}

type StateTrack struct {
	TS     hlc.Timestamp `json:"ts"`
	Origin string        `json:"origin"`
	At     int64         `json:"at"`            // when the stored observation was made
	Own    int64         `json:"own,omitempty"` // latest observation this node made itself, if any
}

// Snapshot returns the replica, this node's own latest observations and the
// counters, taken together so they agree with each other.
func (n *Node) Snapshot() State {
	n.mu.Lock()
	snap := n.tracks.Snapshot()
	tracks := make(map[crdt.ICAO]StateTrack, len(snap))
	for id, t := range snap {
		tracks[id] = StateTrack{TS: t.TS, Origin: t.Origin, At: t.Obs.ObservedAtMS, Own: n.own[id]}
	}
	counters := n.Counters() // the merge and ingest counters only move under mu
	n.mu.Unlock()
	return State{ID: n.id, NowMS: n.trueNow().UnixMilli(), Hash: crdt.HashOf(snap), Counters: counters, Tracks: tracks}
}

func (n *Node) handleState(w http.ResponseWriter, _ *http.Request) { writeJSON(w, n.Snapshot()) }

// Counters returns a copy of the counters.
func (n *Node) Counters() Counters {
	c := &n.c
	return Counters{
		IngestTotal: c.ingestTotal.Load(), IngestLost: c.ingestLost.Load(), IngestHeld: c.ingestHeld.Load(),
		IngestRegress: c.ingestRegress.Load(), SensorErrors: c.sensorErrors.Load(),
		MergeApplied: c.mergeApplied.Load(), MergeStale: c.mergeStale.Load(), MergeRegress: c.mergeRegress.Load(),
		MergeExpired: c.mergeExpired.Load(), Expired: c.expired.Load(), Capped: c.capped.Load(),
		SyncOK: c.syncOK.Load(), SyncErr: c.syncErr.Load(), Rejected: c.rejected.Load(), Aborted: c.aborted.Load(),
		BytesOut: c.bytesOut.Load(), BytesIn: c.bytesIn.Load(),
	}
}

// handleMetrics writes the Prometheus text format by hand: a dozen series do
// not justify the client library, the module's only dependency it would be.
func (n *Node) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	c := n.Counters()
	last := n.clock.Peek()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	series := []struct {
		name, kind, help, labels string
		v                        int64
	}{
		{"holdfast_tracks", "gauge", "Aircraft in this node's picture.", "", int64(n.tracks.Len())},
		{"holdfast_hlc_wall_ms", "gauge", "Wall part of the latest clock stamp.", "", last.WallMS},
		{"holdfast_hlc_logical", "gauge", "Logical part of the latest clock stamp.", "", int64(last.Logical)},
		{"holdfast_ingest_total", "counter", "Local observations ingested.", "", c.IngestTotal},
		{"holdfast_ingest_lost_total", "counter", "Local observations that lost to an older stored one.", "", c.IngestLost},
		{"holdfast_ingest_held_total", "counter", "Local observations that arrived during the startup hold.", "", c.IngestHeld},
		{"holdfast_ingest_regress_total", "counter", "Local observations that replaced a stored one observed later.", "", c.IngestRegress},
		{"holdfast_sensor_errors_total", "counter", "Failed polls of this node's live feed.", "", c.SensorErrors},
		{"holdfast_merge_applied_total", "counter", "Received tracks that won their merge.", "", c.MergeApplied},
		{"holdfast_merge_stale_total", "counter", "Received tracks that lost or were redeliveries.", "", c.MergeStale},
		{"holdfast_merge_regress_total", "counter", "Received tracks that won but were older than what they replaced.", "", c.MergeRegress},
		{"holdfast_merge_expired_total", "counter", "Received tracks that had already expired here.", "", c.MergeExpired},
		{"holdfast_expired_total", "counter", "Aircraft dropped because their track expired.", "", c.Expired},
		{"holdfast_capped_total", "counter", "Tracks for new aircraft refused because the picture was full.", "", c.Capped},
		{"holdfast_sync_requests_total", "counter", "Sync rounds by outcome.", `result="ok"`, c.SyncOK},
		{"holdfast_sync_requests_total", "counter", "", `result="err"`, c.SyncErr},
		{"holdfast_rejected_total", "counter", "Inbound messages refused at the trust boundary.", "", c.Rejected},
		{"holdfast_aborted_total", "counter", "Inbound requests cut off before the whole body arrived.", "", c.Aborted},
		{"holdfast_sync_bytes_total", "counter", "Message body bytes as initiator.", `dir="out"`, c.BytesOut},
		{"holdfast_sync_bytes_total", "counter", "", `dir="in"`, c.BytesIn},
	}
	var b bytes.Buffer
	for _, s := range series {
		if s.help != "" {
			fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", s.name, s.help, s.name, s.kind)
		}
		if s.labels != "" {
			fmt.Fprintf(&b, "%s{%s} %d\n", s.name, s.labels, s.v)
		} else {
			fmt.Fprintf(&b, "%s %d\n", s.name, s.v)
		}
	}
	w.Write(b.Bytes())
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The status line is already sent; the client will see a truncated body.
		slog.Default().Warn("write response", "err", err)
	}
}
