package update

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func settle(t *testing.T, c *Checker) Status {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := c.Status(false)
		if !s.Checking {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("check did not finish")
	return Status{}
}
func TestVersionComparison(t *testing.T) {
	for _, test := range []struct {
		a, b string
		want bool
	}{{"1.10.0", "1.9.9", true}, {"v2.0.0", "1.3.0", true}, {"1.3.0", "1.3.0", false}, {"1.2.9", "1.3.0", false}, {"1.4.0-beta", "1.3.0", false}, {"invalid", "1.3.0", false}, {"1.03.0", "1.2.0", false}} {
		if got := newer(test.a, test.b); got != test.want {
			t.Errorf("%s > %s: %v", test.a, test.b, got)
		}
	}
}
func TestCheckFindsDebAndCachesConcurrentRequests(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/repos/example/fenpei/releases/latest" {
			t.Error(r.URL.Path)
		}
		fmt.Fprint(w, `{"tag_name":"v1.4.0","html_url":"https://github.com/example/fenpei/releases/tag/v1.4.0","body":"更新说明","assets":[{"name":"fenpei_1.4.0-1_amd64.deb","state":"uploaded","browser_download_url":"https://github.com/example/fenpei/releases/download/v1.4.0/fenpei_1.4.0-1_amd64.deb"}]}`)
	}))
	defer upstream.Close()
	c := New("example/fenpei", "1.3.0", nil)
	c.apiBase = upstream.URL
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Status(true) }()
	}
	wg.Wait()
	s := settle(t, c)
	if !s.Available || s.DownloadURL == "" || s.CheckedAt == "" || s.Error != "" {
		t.Fatalf("bad status: %+v", s)
	}
	if calls.Load() != 1 {
		t.Fatalf("duplicate requests: %d", calls.Load())
	}
}
func TestFailedCheckKeepsPreviousUpdateAndRetriesLater(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }))
	defer upstream.Close()
	c := New("example/fenpei", "1.3.0", nil)
	c.apiBase = upstream.URL
	c.state.Latest = "1.4.0"
	c.state.Available = true
	s := settle(t, c)
	if s.Error == "" || !s.Available || s.Latest != "1.4.0" {
		t.Fatalf("lost cached update: %+v", s)
	}
	if c.interval != 5*time.Minute {
		t.Fatal("failure retry interval missing")
	}
}
func TestRelayRecomputesUpdateForLocalVersion(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"current_version":"1.4.0","latest_version":"1.4.0","repository":"example/fenpei","available":false,"release_url":"https://github.com/example/fenpei/releases/tag/v1.4.0"}`)
	}))
	defer relay.Close()
	c := New("example/fenpei", "1.3.0", func() string { return relay.URL })
	if s := settle(t, c); !s.Available || s.Current != "1.3.0" {
		t.Fatalf("local version ignored: %+v", s)
	}
}
func TestPendingRelayDoesNotGetStuckChecking(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"repository":"example/fenpei","checking":true}`)
	}))
	defer relay.Close()
	c := New("example/fenpei", "1.3.0", func() string { return relay.URL })
	s := settle(t, c)
	if s.Checking || s.Error == "" || c.interval != 10*time.Second {
		t.Fatalf("pending check stuck: %+v", s)
	}
}
func TestUnsafeLinksAndInvalidRepositoryRejected(t *testing.T) {
	for _, link := range []string{"javascript:alert(1)", "http://github.com/example/fenpei/releases/x", "https://evil.example/releases/x", "https://github.com/other/repo/releases/x", "https://github.com@example.org/example/fenpei/releases/x"} {
		if err := validateLinks(Status{Repository: "example/fenpei", DownloadURL: link}); err == nil {
			t.Fatalf("accepted %s", link)
		}
	}
	c := New("bad/repo/extra", "1.3.0", nil)
	if s := settle(t, c); s.Error == "" {
		t.Fatal("invalid repo accepted")
	}
}
