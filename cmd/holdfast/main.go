// Command holdfast runs one edge node.
//
//	holdfast -id a -peers http://b:8080,http://c:8080 \
//	    -trace traces/seed1.jsonl -coverage 39,-77,55
//
// A cloud peer goes in -cloud-peer, with its token in HOLDFAST_CLOUD_TOKEN.
// No other peer is ever sent the token:
//
//	HOLDFAST_CLOUD_TOKEN=... holdfast -id a \
//	    -cloud-peer https://abc123.execute-api.us-east-1.amazonaws.com \
//	    -live-url https://api.adsb.lol/v2/point -coverage 39,-77,60
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/haeganm/holdfast/internal/adsb"
	"github.com/haeganm/holdfast/internal/crdt"
	"github.com/haeganm/holdfast/internal/node"
	"github.com/haeganm/holdfast/internal/replay"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "holdfast:", err)
		os.Exit(1)
	}
}

// run starts the node and serves until ctx is done.
func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("holdfast", flag.ContinueOnError)
	var (
		id       = fs.String("id", "", "node id, also the origin of its writes (required)")
		listen   = fs.String("listen", ":8080", "listen address")
		peers    = fs.String("peers", "", "comma-separated peer base URLs")
		cloud    = fs.String("cloud-peer", "", "base URL of a cloud peer, which alone gets the token in HOLDFAST_CLOUD_TOKEN; https, or http to a private IP")
		interval = fs.Duration("sync-interval", time.Second, "sync period per peer")
		timeout  = fs.Duration("sync-timeout", 2*time.Second, "client timeout per sync request")
		clock    = fs.String("clock", "hlc", "hlc, or wall for the wall-clock baseline")
		offsetMS = fs.Int("clock-offset-ms", 0, "skew this node's physical clock (experiments)")
		maxDrift = fs.Duration("max-drift", 5*time.Second, "reject remote stamps further ahead than this")
		ttl      = fs.Duration("track-ttl", 5*time.Minute, "drop an aircraft whose track is older than this; negative keeps everything")
		hold     = fs.Duration("startup-hold", 0, "hold own reports this long after start, so a restart can't reuse old stamps; 0 means the max drift, negative none")
		trace    = fs.String("trace", "", "trace to replay (.jsonl or .jsonl.gz); empty means no local ingest")
		from     = fs.Int64("trace-from-ms", 0, "replay the trace from this t_ms, shifted to start at 0")
		live     = fs.String("live-url", "", "ingest live from a readsb-format feed instead; the coverage's lat/lon/radius_nm is appended")
		every    = fs.Duration("live-interval", 3*time.Second, "how often to poll the live feed")
		offset   = fs.Duration("live-offset", 0, "wait this long before the first poll, to spread several nodes' polls")
		coverage = fs.String("coverage", "", "lat,lon,radius_km of the reports this node receives")
		t0       = fs.Int64("t0", 0, "unix ms at which ingest starts; 0 means 2 s from now")
		traceMS  = fs.Int64("trace-ms", 0, "ingest this many ms (of the trace, or live); 0 means all")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *clock != "hlc" && *clock != "wall" {
		return fmt.Errorf("-clock %q: want hlc or wall", *clock)
	}
	if *trace != "" && *live != "" {
		return errors.New("give -trace or -live-url, not both")
	}
	if *every <= 0 {
		return fmt.Errorf("-live-interval %v: must be positive", *every)
	}
	if (*trace == "" && *live == "") != (*coverage == "") {
		return errors.New("-coverage goes with -trace or -live-url")
	}
	if *timeout <= 0 {
		return fmt.Errorf("-sync-timeout %v: must be positive", *timeout)
	}
	if *traceMS < 0 {
		return fmt.Errorf("-trace-ms %d: must not be negative", *traceMS)
	}
	if *from < 0 {
		return fmt.Errorf("-trace-from-ms %d: must not be negative", *from)
	}
	// From a start long past, every report would be due at once.
	if late := time.Since(time.UnixMilli(*t0)); *t0 != 0 && late > 5*time.Second {
		return fmt.Errorf("-t0 %d is %v in the past", *t0, late.Round(time.Second))
	}
	if *live != "" {
		if _, err := baseURL("-live-url", *live); err != nil {
			return err
		}
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	peerList, err := peerURLs(*peers)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: *timeout}
	if *cloud != "" {
		u, err := cloudURL(*cloud)
		if err != nil {
			return err
		}
		token := os.Getenv("HOLDFAST_CLOUD_TOKEN")
		if token == "" {
			return errors.New("-cloud-peer needs its token in HOLDFAST_CLOUD_TOKEN")
		}
		base := strings.TrimRight(*cloud, "/")
		for _, p := range peerList {
			if pu, err := url.Parse(p); err == nil && sameHost(pu, u) {
				return fmt.Errorf("%s in -peers is the cloud peer; give it only in -cloud-peer", p)
			}
		}
		// Over http the token is in the clear, so the cloud peer's requests
		// never go through a proxy; over https a proxy sees only a CONNECT.
		via := http.DefaultTransport
		if u.Scheme == "http" {
			direct := http.DefaultTransport.(*http.Transport).Clone()
			direct.Proxy = nil
			via = direct
		}
		client.Transport = bearer{next: http.DefaultTransport, cloud: via, scheme: u.Scheme, host: u.Host, token: token}
		peerList = append(peerList, base)
	}
	n, err := node.New(node.Config{
		ID:           *id,
		Peers:        peerList,
		SyncInterval: *interval,
		Wall:         *clock == "wall",
		ClockOffset:  time.Duration(*offsetMS) * time.Millisecond,
		MaxDrift:     *maxDrift,
		StartupHold:  *hold,
		TrackTTL:     *ttl,
		Client:       client,
		Logger:       log,
	})
	if err != nil {
		return err
	}

	var recs []replay.Record
	var circle replay.Circle
	if *coverage != "" {
		if circle, err = replay.ParseCircle(*coverage); err != nil {
			return err
		}
	}
	if *trace != "" {
		all, err := replay.Open(*trace)
		if err != nil {
			return err
		}
		// A window can hold no reports in this node's coverage, as some
		// windows of the recorded hour do for some sensors, but it has to
		// start inside the trace.
		if len(all) == 0 || *from > all[len(all)-1].TMS {
			return fmt.Errorf("-trace-from-ms %d: %s has no records from there on", *from, *trace)
		}
		for _, r := range replay.Window(all, *from, *traceMS) {
			if r.For(*id) && circle.Contains(r.Lat, r.Lon) {
				recs = append(recs, r)
			}
		}
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           n.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	go n.Run(ctx)

	start := time.Now().Add(2 * time.Second)
	if *t0 != 0 {
		start = time.UnixMilli(*t0)
	}
	if *live != "" {
		go ingestLive(ctx, n, log, circle, *live, *every, *offset, start, time.Duration(*traceMS)*time.Millisecond)
	}
	if *trace != "" {
		log.Info("replay scheduled", "records_in_coverage", len(recs), "start", start.UnixMilli())
		go func() {
			var maxLate time.Duration
			err := replay.Play(ctx, recs, start, func(r replay.Record) {
				due := start.Add(time.Duration(r.TMS) * time.Millisecond)
				maxLate = max(maxLate, time.Since(due))
				n.Ingest(crdt.ICAO(r.ICAO), crdt.Observation{
					Lat: r.Lat, Lon: r.Lon, AltFt: r.AltFt, GSKt: r.GSKt, HeadingDeg: r.HeadingDeg,
					ObservedAtMS: due.UnixMilli() - r.DelayMS, // a recorded report arrived after it was made
				})
			})
			if err == nil {
				log.Info("replay done", "records", len(recs), "max_late_ms", maxLate.Milliseconds())
			}
		}()
	}

	log.Info("listening", "addr", *listen, "peers", len(peerList), "clock", *clock, "clock_offset_ms", *offsetMS)
	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}
	return shutdown(srv, 5*time.Second, log)
}

// shutdown stops srv, giving open requests up to budget to finish. A peer's
// upload still arriving over a slow link can outlast it (the read timeout is
// longer), so then the rest are cut off: a peer's next round resends
// whatever didn't land, so a stop never loses anything.
func shutdown(srv *http.Server, budget time.Duration, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	err := srv.Shutdown(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		log.Warn("shutdown forced: requests were still open", "after", budget)
		srv.Close()
		return nil
	}
	return err
}

// ingestLive polls the feed for this node's coverage from start until length
// has passed (0: until shutdown) and ingests every new observation inside the
// circle. A feed outage is counted and logged, never fatal: the node keeps
// syncing what it has.
func ingestLive(ctx context.Context, n *node.Node, log *slog.Logger, circle replay.Circle,
	base string, every, offset time.Duration, start time.Time, length time.Duration) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Until(start) + offset):
	}
	if length > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, start.Add(length))
		defer cancel()
	}
	feed := adsb.Feed{
		URL:   adsb.PointURL(base, circle.Lat, circle.Lon, circle.RadiusKm),
		Every: every, MaxSkew: 30 * time.Second, Client: &http.Client{Timeout: 10 * time.Second}, Now: time.Now,
		Agent: "holdfast-node (github.com/haeganm/holdfast)",
	}
	log.Info("live ingest", "url", feed.URL, "every", every)
	sensor := adsb.NewSensor()
	feed.Follow(ctx, func(rs []adsb.Report) {
		inside := slices.DeleteFunc(rs, func(r adsb.Report) bool { return !circle.Contains(r.Obs.Lat, r.Obs.Lon) })
		for _, r := range sensor.New(inside) {
			n.Ingest(r.ICAO, r.Obs)
		}
	}, func(err error) {
		n.SensorFailed()
		log.Warn("live feed", "err", err)
	})
	log.Info("live ingest stopped")
}

// baseURL checks a URL that request paths are appended to: http or https,
// with a host, and nothing a path can't follow. A user's password would also
// end up in the logs.
func baseURL(name, raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || strings.ContainsAny(raw, "?#") {
		return nil, fmt.Errorf("%s %q: want an http or https base URL, with a host and no user, query or fragment", name, raw)
	}
	return u, nil
}

// peerURLs parses -peers, each peer once: a second sync loop to the same
// peer would only double the traffic.
func peerURLs(list string) ([]string, error) {
	var peers []string
	var seen []*url.URL
	for p := range strings.SplitSeq(list, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		u, err := baseURL("-peers", p)
		if err != nil {
			return nil, err
		}
		if slices.ContainsFunc(seen, func(s *url.URL) bool { return sameHost(s, u) }) {
			return nil, fmt.Errorf("-peers names %s twice", u.Host)
		}
		seen = append(seen, u)
		peers = append(peers, strings.TrimRight(p, "/"))
	}
	return peers, nil
}

// cloudURL checks a -cloud-peer URL. The token goes with every request to it,
// so the link must be https, unless the peer has a private IP address, as the
// harness's local cloud peer does.
func cloudURL(raw string) (*url.URL, error) {
	u, err := baseURL("-cloud-peer", raw)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(u.Hostname())
	private := ip != nil && (ip.IsPrivate() || ip.IsLoopback())
	if u.Scheme == "http" && !private {
		return nil, fmt.Errorf("-cloud-peer %q: the token would cross the network in the clear; use https (http only to a private IP)", raw)
	}
	return u, nil
}

// sameHost reports whether two URLs name the same host and port, ignoring
// case and a scheme's default port.
func sameHost(a, b *url.URL) bool {
	port := func(u *url.URL) string {
		if p := u.Port(); p != "" {
			return p
		}
		if strings.EqualFold(u.Scheme, "https") {
			return "443"
		}
		return "80"
	}
	return strings.EqualFold(a.Hostname(), b.Hostname()) && port(a) == port(b)
}

// bearer adds the cloud peer's token to requests for that peer and no other,
// and sends those through cloud. Nothing authenticates edge peers, so a token
// sent to one would be anyone's.
type bearer struct {
	next, cloud         http.RoundTripper
	scheme, host, token string
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != b.scheme || r.URL.Host != b.host {
		return b.next.RoundTrip(r)
	}
	r = r.Clone(r.Context()) // a RoundTripper must not change the caller's request
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.cloud.RoundTrip(r)
}
