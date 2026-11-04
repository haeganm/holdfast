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
	"strings"
	"syscall"
	"time"

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
		clock    = flag.String("clock", "hlc", "hlc, or wall for the wall-clock baseline")
		offsetMS = flag.Int("clock-offset-ms", 0, "skew this node's physical clock (experiments)")
		maxDrift = flag.Duration("max-drift", 5*time.Second, "reject remote stamps further ahead than this")
		trace    = flag.String("trace", "", "trace to replay; empty means no local ingest")
		coverage = flag.String("coverage", "", "lat,lon,radius_km of the reports this node receives")
		t0       = flag.Int64("t0", 0, "unix ms at which the trace starts; 0 means 2 s from now")
		traceMS  = flag.Int64("trace-ms", 0, "replay only records before this t_ms; 0 means all")
	)
	flag.Parse()
	if *clock != "hlc" && *clock != "wall" {
		return fmt.Errorf("-clock %q: want hlc or wall", *clock)
	}
	if (*trace == "") != (*coverage == "") {
		return errors.New("-trace and -coverage go together")
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
		Logger:       log,
	})
	if err != nil {
		return err
	}

	var recs []replay.Record
	var circle replay.Circle
	if *trace != "" {
		if circle, err = replay.ParseCircle(*coverage); err != nil {
			return err
		}
		f, err := os.Open(*trace)
		if err != nil {
			return err
		}
		all, err := replay.Read(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", *trace, err)
		}
		for _, r := range all {
			if (*traceMS == 0 || r.TMS < *traceMS) && circle.Contains(r.Lat, r.Lon) {
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

	if *trace != "" {
		start := time.Now().Add(2 * time.Second)
		if *t0 != 0 {
			start = time.UnixMilli(*t0)
		}
		log.Info("replay scheduled", "records_in_coverage", len(recs), "start", start.UnixMilli())
		go func() {
			var maxLate time.Duration
			err := replay.Play(ctx, recs, start, func(r replay.Record) {
				due := start.Add(time.Duration(r.TMS) * time.Millisecond)
				maxLate = max(maxLate, time.Since(due))
				n.Ingest(crdt.ICAO(r.ICAO), crdt.Observation{
					Lat: r.Lat, Lon: r.Lon, AltFt: r.AltFt, GSKt: r.GSKt, HeadingDeg: r.HeadingDeg,
					ObservedAtMS: due.UnixMilli(),
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
