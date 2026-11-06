package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
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
	} {
		if _, err := cloudURL(raw); (err == nil) != ok {
			t.Errorf("cloudURL(%q) = %v, want accepted=%v", raw, err, ok)
		}
	}
}
