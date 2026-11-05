// Command gentrace writes a trace: either a deterministic synthetic one, or a
// recording of what each node would have received from a live ADS-B feed.
//
//	gentrace -seed 1 -aircraft 40 -duration 60s -out traces/seed1.jsonl
//	gentrace -sensor a=39.0,-77.0,60 -sensor b=39.0,-76.2,60 -sensor c=39.0,-77.8,60 \
//	    -duration 1h -out traces/adsb-2026-11-05.jsonl.gz
//	gentrace -finalize traces/adsb-2026-11-05.jsonl.gz   # after a crash, keep what was recorded
//
// A recording makes one request stream for all sensors, over a circle that
// encloses them, and hands each response to the next sensor in turn: three
// sensors at -every 6s make one request every 2 s, which adsb.lol accepted
// without refusals when this was written (faster drew 429s). Each report is
// written to <out>.partial as it arrives, so a killed recording loses at
// most the last minute; -finalize turns the partial file into the sorted,
// gzipped trace.
package main

import (
	"bufio"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/haeganm/holdfast/internal/adsb"
	"github.com/haeganm/holdfast/internal/replay"
)

type sensors []sensor

type sensor struct {
	id     string
	circle replay.Circle
}

func (s *sensors) String() string { return fmt.Sprint(*s) }

// Set parses id=lat,lon,radius_km.
func (s *sensors) Set(v string) error {
	id, circle, ok := strings.Cut(v, "=")
	if !ok || id == "" {
		return fmt.Errorf("sensor %q: want id=lat,lon,radius_km", v)
	}
	c, err := replay.ParseCircle(circle)
	if err != nil {
		return err
	}
	if slices.ContainsFunc(*s, func(o sensor) bool { return o.id == id }) {
		return fmt.Errorf("sensor %q given twice", id)
	}
	*s = append(*s, sensor{id, c})
	return nil
}

func main() {
	seed := flag.Uint64("seed", 1, "random seed (synthetic)")
	aircraft := flag.Int("aircraft", 40, "number of aircraft (synthetic)")
	dur := flag.Duration("duration", 60*time.Second, "trace length")
	out := flag.String("out", "", "output path, gzipped if it ends in .gz")
	var sens sensors
	flag.Var(&sens, "sensor", "record a live feed for this node, id=lat,lon,radius_km (repeatable)")
	feed := flag.String("feed", "https://api.adsb.lol/v2/point", "readsb-format feed; the sensor's lat/lon/radius_nm is appended")
	every := flag.Duration("every", 6*time.Second, "how often each sensor observes")
	finalize := flag.String("finalize", "", "turn PATH.partial left by a killed recording into PATH")
	flag.Parse()

	var err error
	switch {
	case *finalize != "":
		err = finish(*finalize)
	case *out == "" || *dur <= 0:
		flag.Usage()
		os.Exit(2)
	case len(sens) > 0:
		err = record(sens, *feed, *every, *dur, *out)
	case *aircraft < 1:
		flag.Usage()
		os.Exit(2)
	default:
		err = write(*out, replay.Generate(*seed, *aircraft, *dur))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "gentrace:", err)
		os.Exit(1)
	}
}

func record(sens sensors, feedURL string, every, dur time.Duration, out string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	partial, err := os.OpenFile(out+".partial", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("%w (finish or remove the earlier recording first)", err)
	}
	w := bufio.NewWriter(partial)
	enc := json.NewEncoder(w)

	// One request stream for every sensor: a public feed's allowance is per
	// client, and separate pollers would compete for it and starve each
	// other. Each response goes to the next sensor in turn, so each sensor
	// still observes at its own moments, every -every.
	circles := make([]replay.Circle, len(sens))
	dedupe := make([]*adsb.Sensor, len(sens))
	for i, s := range sens {
		circles[i], dedupe[i] = s.circle, adsb.NewSensor()
	}
	all := replay.Enclosing(circles)
	feed := adsb.Feed{
		URL: adsb.PointURL(feedURL, all.Lat, all.Lon, all.RadiusKm), Every: every / time.Duration(len(sens)),
		MaxSkew: 30 * time.Second, Client: &http.Client{Timeout: 10 * time.Second}, Now: time.Now,
		Agent: "holdfast-recorder (github.com/haeganm/holdfast)",
	}
	fmt.Fprintf(os.Stderr, "polling %s every %v\n", feed.URL, feed.Every)
	start := time.Now()
	shown := start
	records := map[string]int{}
	polls, failures := 0, 0
	feed.Follow(ctx, func(rs []adsb.Report) {
		i := polls % len(sens)
		polls++
		s, now := sens[i], time.Now()
		inside := slices.DeleteFunc(rs, func(r adsb.Report) bool { return !s.circle.Contains(r.Obs.Lat, r.Obs.Lon) })
		for _, r := range dedupe[i].New(inside) {
			rec := replay.Record{
				TMS: now.Sub(start).Milliseconds(), ICAO: string(r.ICAO),
				Lat: r.Obs.Lat, Lon: r.Obs.Lon, AltFt: r.Obs.AltFt, GSKt: r.Obs.GSKt, HeadingDeg: r.Obs.HeadingDeg,
				Node: s.id,
				// The observation time is on the feed's clock; a feed running
				// ahead of ours would make the delay negative.
				DelayMS: min(max(0, now.UnixMilli()-r.Obs.ObservedAtMS), replay.MaxDelay.Milliseconds()),
			}
			if enc.Encode(rec) == nil {
				records[s.id]++
			}
		}
		if now.Sub(shown) >= time.Minute {
			shown = now
			w.Flush() // so a killed recording keeps everything up to the last minute
			fmt.Fprintf(os.Stderr, "%s polls %d, failed %d, recorded %v\n", now.Sub(start).Round(time.Second), polls, failures, records)
		}
	}, func(err error) {
		failures++
		fmt.Fprintf(os.Stderr, "%s %v\n", time.Now().Format(time.TimeOnly), err)
	})
	err = errors.Join(w.Flush(), partial.Close())
	fmt.Fprintf(os.Stderr, "done after %s: polls %d, failed %d, recorded %v\n", time.Since(start).Round(time.Second), polls, failures, records)
	if err != nil {
		return err
	}
	return finish(out)
}

// finish sorts what a recording wrote to out.partial and writes out.
func finish(out string) error {
	f, err := os.Open(out + ".partial")
	if err != nil {
		return err
	}
	var recs []replay.Record
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r replay.Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			break // the last line of a killed recording can be cut short
		}
		recs = append(recs, r)
	}
	f.Close()
	if err := sc.Err(); err != nil {
		return err
	}
	slices.SortStableFunc(recs, func(a, b replay.Record) int {
		return cmp.Or(cmp.Compare(a.TMS, b.TMS), strings.Compare(a.Node, b.Node), strings.Compare(a.ICAO, b.ICAO))
	})
	if err := write(out, recs); err != nil {
		return err
	}
	if _, err := replay.Open(out); err != nil {
		return fmt.Errorf("the finished trace does not read back: %w", err)
	}
	fmt.Fprintf(os.Stderr, "wrote %d records to %s\n", len(recs), out)
	return os.Remove(out + ".partial")
}

// write replaces path atomically, so a failed run never leaves half a trace.
func write(path string, recs []replay.Record) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".gentrace-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	var w io.Writer = tmp
	gz := gzip.NewWriter(tmp)
	if strings.HasSuffix(path, ".gz") {
		w = gz
	}
	err = replay.Write(w, recs)
	if w == io.Writer(gz) {
		err = errors.Join(err, gz.Close())
	}
	if err = errors.Join(err, tmp.Close()); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
