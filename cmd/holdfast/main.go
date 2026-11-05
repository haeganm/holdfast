// Command holdfast runs one edge node.
//
//	holdfast -id a -peers http://b:8080,http://c:8080 \
//	    -trace traces/seed1.jsonl -coverage 39,-77,55
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
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
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "holdfast:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id       = flag.String("id", "", "node id, also the origin of its writes (required)")
		listen   = flag.String("listen", ":8080", "listen address")
		peers    = flag.String("peers", "", "comma-separated peer base URLs")
		interval = flag.Duration("sync-interval", time.Second, "sync period per peer")
		timeout  = flag.Duration("sync-timeout", 2*time.Second, "client timeout per sync request")
		clock    = flag.String("clock", "hlc", "hlc, or wall for the wall-clock baseline")
		offsetMS = flag.Int("clock-offset-ms", 0, "skew this node's physical clock (experiments)")
		maxDrift = flag.Duration("max-drift", 5*time.Second, "reject remote stamps further ahead than this")
		ttl      = flag.Duration("track-ttl", 5*time.Minute, "drop an aircraft whose track is older than this; negative keeps everything")
		hold     = flag.Duration("startup-hold", 0, "hold own reports this long after start, so a restart can't reuse old stamps; 0 means the max drift, negative none")
		trace    = flag.String("trace", "", "trace to replay (.jsonl or .jsonl.gz); empty means no local ingest")
		from     = flag.Int64("trace-from-ms", 0, "replay the trace from this t_ms, shifted to start at 0")
		live     = flag.String("live-url", "", "ingest live from a readsb-format feed instead; the coverage's lat/lon/radius_nm is appended")
		every    = flag.Duration("live-interval", 3*time.Second, "how often to poll the live feed")
		offset   = flag.Duration("live-offset", 0, "wait this long before the first poll, to spread several nodes' polls")
		coverage = flag.String("coverage", "", "lat,lon,radius_km of the reports this node receives")
		t0       = flag.Int64("t0", 0, "unix ms at which ingest starts; 0 means 2 s from now")
		traceMS  = flag.Int64("trace-ms", 0, "ingest this many ms (of the trace, or live); 0 means all")
	)
	flag.Parse()
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

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	var peerList []string
	for p := range strings.SplitSeq(*peers, ",") {
		if p = strings.TrimSpace(p); p != "" {
			peerList = append(peerList, strings.TrimRight(p, "/"))
		}
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
		Client:       &http.Client{Timeout: *timeout},
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
		for _, r := range replay.Window(all, *from, *traceMS) {
			if r.For(*id) && circle.Contains(r.Lat, r.Lon) {
				recs = append(recs, r)
			}
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
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
