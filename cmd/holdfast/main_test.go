package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// peer records the Authorization header of every request it gets, and
// answers each one as a peer with an empty picture would.
type peer struct {
	*httptest.Server
	mu   sync.Mutex
	auth []string
}

func newPeer(t *testing.T) *peer {
	p := &peer{}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.auth = append(p.auth, r.Header.Get("Authorization"))
		p.mu.Unlock()
		io.WriteString(w, "{}")
	}))
	t.Cleanup(p.Close)
	return p
}

func (p *peer) seen() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.auth)
}

// runUntil runs the edge with args until done reports true, then stops it.
func runUntil(t *testing.T, done func() bool, args ...string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() {
		exited <- run(ctx, append([]string{"-id", "a", "-listen", "127.0.0.1:0", "-sync-interval", "10ms"}, args...))
	}()
	for deadline := time.Now().Add(10 * time.Second); !done(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("still waiting after 10 s; run returned %v", <-exited)
		}
	}
	cancel()
	if err := <-exited; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestTheTokenGoesOnlyToTheCloudPeer(t *testing.T) {
	cloud, edge := newPeer(t), newPeer(t) // same address, different ports
	t.Setenv("HOLDFAST_CLOUD_TOKEN", "tok")
	runUntil(t, func() bool { return len(cloud.seen()) > 0 && len(edge.seen()) > 0 },
		"-peers", edge.URL, "-cloud-peer", cloud.URL)
	if c, e := cloud.seen(), edge.seen(); slices.ContainsFunc(c, func(a string) bool { return a != "Bearer tok" }) ||
		slices.ContainsFunc(e, func(a string) bool { return a != "" }) {
		t.Fatalf("the cloud peer got %q and an edge peer got %q; only the cloud peer should get the token", c, e)
	}
}

// Over http the token is in the clear, so a request to the cloud peer must
// never go through a proxy, even one the environment sets. (Over https a
// proxy would see only a CONNECT.)
func TestTheTokenNeverCrossesAProxyInTheClear(t *testing.T) {
	proxy, cloud := newPeer(t), newPeer(t)
	via, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	saved := http.DefaultTransport
	http.DefaultTransport = &http.Transport{Proxy: http.ProxyURL(via)} // what HTTP_PROXY would make of it
	t.Cleanup(func() { http.DefaultTransport = saved })
	t.Setenv("HOLDFAST_CLOUD_TOKEN", "tok")
	runUntil(t, func() bool { return len(cloud.seen()) > 0 || len(proxy.seen()) > 0 }, "-cloud-peer", cloud.URL)
	if got := proxy.seen(); len(got) > 0 {
		t.Fatalf("a proxy got the cloud peer's requests, with Authorization %q", got)
	}
}

func writeTrace(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const twoRecords = `{"t_ms":0,"icao":"A1B2C3","lat":39,"lon":-77,"alt":0,"gs":0,"hdg":0}
{"t_ms":1000,"icao":"A1B2C3","lat":39,"lon":-77,"alt":0,"gs":0,"hdg":0}`

// Each of these used to start a node that quietly did nothing useful.
func TestBadFlagsAreRefusedAtStartup(t *testing.T) {
	trace := writeTrace(t, twoRecords)
	t.Setenv("HOLDFAST_CLOUD_TOKEN", "tok")
	stopped, cancel := context.WithCancel(context.Background())
	cancel() // a node that does start stops at once
	for _, c := range []struct {
		why, want string
		args      []string
	}{
		{"a peer with no scheme", "-peers", []string{"-peers", "b:8080"}},
		{"a peer that isn't http", "-peers", []string{"-peers", "ftp://b:8080"}},
		{"a peer with a query", "-peers", []string{"-peers", "http://b:8080?x=1"}},
		{"the same peer twice", "twice", []string{"-peers", "http://b:8080,http://B:8080/"}},
		{"no sync timeout", "-sync-timeout", []string{"-sync-timeout", "0"}},
		{"a negative sync timeout", "-sync-timeout", []string{"-sync-timeout", "-1s"}},
		{"a negative trace length", "-trace-ms", []string{"-trace", trace, "-coverage", "39,-77,60", "-trace-ms", "-1"}},
		{"a negative trace offset", "-trace-from-ms", []string{"-trace", trace, "-coverage", "39,-77,60", "-trace-from-ms", "-1"}},
		{"a trace offset past the last record", "-trace-from-ms", []string{"-trace", trace, "-coverage", "39,-77,60", "-trace-from-ms", "1001"}},
		{"a start in 1970", "-t0", []string{"-trace", trace, "-coverage", "39,-77,60", "-t0", "1"}},
		{"a live feed with no scheme", "-live-url", []string{"-live-url", "api.adsb.lol/v2/point", "-coverage", "39,-77,60"}},
		{"NaN coverage", "coverage", []string{"-trace", trace, "-coverage", "NaN,NaN,60"}},
	} {
		err := run(stopped, append([]string{"-id", "a", "-listen", "127.0.0.1:0"}, c.args...))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: run returned %v, want an error about %s", c.why, err, c.want)
		}
	}
}

// What the harness passes, a window with no reports for this node's coverage
// among it, still starts.
func TestTheHarnessFlagsStillStart(t *testing.T) {
	trace := writeTrace(t, twoRecords)
	t.Setenv("HOLDFAST_CLOUD_TOKEN", "tok")
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	t0 := "-t0=" + strconv.FormatInt(time.Now().Add(10*time.Second).UnixMilli(), 10)
	for _, args := range [][]string{
		{"-peers=http://172.30.0.12:8080,http://172.30.0.13:8080", "-cloud-peer=", "-clock=hlc", "-sync-timeout=2s",
			"-clock-offset-ms=0", "-trace=" + trace, "-trace-from-ms=1000", "-live-interval=18s", "-live-offset=0s",
			"-coverage=39.0,-77.0,60", t0, "-trace-ms=50000"},
		{"-peers=", "-cloud-peer=http://172.30.0.14:8080", "-live-url=https://api.adsb.lol/v2/point",
			"-trace-from-ms=0", "-live-offset=6s", "-coverage=39.0,-76.2,60", t0, "-trace-ms=50000"},
		{"-trace=" + trace, "-coverage=10,10,1", t0},
		{"-peers=", "-clock=hlc", "-sync-timeout=2s"}, // the star's hub
	} {
		if err := run(stopped, append([]string{"-id", "a", "-listen", "127.0.0.1:0"}, args...)); err != nil {
			t.Errorf("%q: %v", args, err)
		}
	}
}

// A peer's upload still arriving over a slow link can outlast the shutdown
// budget, since the read timeout is longer. That is a routine stop, not a
// failure: the upload is cut off, and the peer's next round resends it.
func TestShutdownCutsOffAnUploadThatOutlastsItsBudget(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	reading := make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(reading)
		io.Copy(io.Discard, r.Body)
	})}
	go srv.Serve(ln)
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	io.WriteString(conn, "POST /push HTTP/1.1\r\nHost: a\r\nContent-Length: 1000\r\n\r\n{\"from\":\"b\",") // the rest never comes
	select {
	case <-reading:
	case <-time.After(10 * time.Second):
		t.Fatal("the upload never reached its handler")
	}
	var logs strings.Builder
	if err := shutdown(srv, 100*time.Millisecond, slog.New(slog.NewTextHandler(&logs, nil))); err != nil {
		t.Fatalf("shutdown with an upload still arriving: %v", err)
	}
	if !strings.Contains(logs.String(), "forced") {
		t.Fatalf("the forced close was not logged: %q", logs.String())
	}
}

func TestSameHostCatchesTheCloudPeerListedTwice(t *testing.T) {
	parse := func(s string) *url.URL {
		u, err := url.Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	cloud := parse("https://abc123.execute-api.us-east-1.amazonaws.com")
	for raw, same := range map[string]bool{
		"https://ABC123.execute-api.us-east-1.amazonaws.com":          true,
		"https://abc123.execute-api.us-east-1.amazonaws.com:443/prod": true,
		"http://abc123.execute-api.us-east-1.amazonaws.com":           false, // port 80
		"https://other.example":                                       false,
	} {
		if sameHost(parse(raw), cloud) != same {
			t.Errorf("sameHost(%q, cloud) = %v, want %v", raw, !same, same)
		}
	}
}

func TestCloudURLKeepsTheTokenOffCleartextLinks(t *testing.T) {
	for raw, ok := range map[string]bool{
		"https://abc123.execute-api.us-east-1.amazonaws.com": true,
		"http://172.30.0.14:8080":                            true, // the harness's local cloud peer
		"http://127.0.0.1:8085":                              true,
		"http://abc123.execute-api.us-east-1.amazonaws.com":  false,
		"http://8.8.8.8":                                     false,
		"http://localhost:8085":                              false, // a name could resolve anywhere
		"ftp://172.30.0.14":                                  false,
		"172.30.0.14:8080":                                   false,
		"https://":                                           false,
		"https://abc.example/prod":                           true,  // a stage path is fine
		"https://abc.example/prod?stage=1":                   false, // requests would go to ...?stage=1/sync
		"https://abc.example/#x":                             false,
		"http://10.0.0.1:8080?":                              false,
		"https://u:pw@abc.example":                           false, // the password would land in the logs
	} {
		if _, err := cloudURL(raw); (err == nil) != ok {
			t.Errorf("cloudURL(%q) = %v, want accepted=%v", raw, err, ok)
		}
	}
}
