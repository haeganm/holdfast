// Command gentrace writes a deterministic synthetic trace.
//
//	gentrace -seed 1 -aircraft 40 -duration 60s -out traces/seed1.jsonl
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/haeganm/holdfast/internal/replay"
)

func main() {
	seed := flag.Uint64("seed", 1, "random seed")
	aircraft := flag.Int("aircraft", 40, "number of aircraft")
	dur := flag.Duration("duration", 60*time.Second, "trace length")
	out := flag.String("out", "", "output path (required)")
	flag.Parse()
	if *out == "" || *aircraft < 1 || *dur <= 0 {
		flag.Usage()
		os.Exit(2)
	}
	if err := write(*out, replay.Generate(*seed, *aircraft, *dur)); err != nil {
		fmt.Fprintln(os.Stderr, "gentrace:", err)
		os.Exit(1)
	}
}

// write replaces path atomically, so a failed run never leaves half a trace.
func write(path string, recs []replay.Record) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".gentrace-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := replay.Write(tmp, recs); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
