package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestTheTokenGoesOnlyToTheCloudPeer(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{}
	serve := func(name string) *httptest.Server {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			got[name] = r.Header.Get("Authorization")
		}))
		t.Cleanup(s.Close)
		return s
	}
	cloud, edge := serve("cloud"), serve("edge") // same address, different ports
	u, err := cloudURL(cloud.URL)
	if err != nil {
		t.Fatal(err)
	}
	c := &http.Client{Transport: bearer{next: http.DefaultTransport, scheme: u.Scheme, host: u.Host, token: "tok"}}
	for _, s := range []*httptest.Server{cloud, edge} {
		resp, err := c.Post(s.URL+"/sync", "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if got["cloud"] != "Bearer tok" || got["edge"] != "" {
		t.Fatalf("the cloud peer got %q and an edge peer got %q; only the cloud peer should get the token", got["cloud"], got["edge"])
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
