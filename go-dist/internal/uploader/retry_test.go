package uploader

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryLimitAgainstFailingPlatform(t *testing.T) {
	var requests atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "platform unavailable", 503)
	}))
	defer platform.Close()
	t.Setenv("FENPEI_PLATFORM_PASSWORD", "test-password")
	t.Setenv("FENPEI_DAG_ACCESS_TOKEN", "test-token")
	t.Setenv("FENPEI_BASE_URL", platform.URL)
	dir := t.TempDir()
	data := filepath.Join(dir, "data.txt")
	zip := filepath.Join(dir, "JZ08-task.zip")
	for _, path := range []string{data, zip} {
		if err := os.WriteFile(path, []byte("keep me"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	u := New(Config{MaxRetries: 5, LogDir: filepath.Join(dir, "logs"), OutputDir: dir})
	var finalStatus string
	var waits []time.Duration
	ok, msg := u.runUpload("JZ08-task", func(_, status, message string) { finalStatus = status }, nil,
		func(retry int) (bool, string, string) {
			step := ""
			ok, msg := u.doUpload(dir, "JZ08-task", nil, nil, retry, &step)
			return ok, msg, step
		},
		func(delay time.Duration) bool { waits = append(waits, delay); return false })
	if ok || msg == "" || finalStatus != "retry_exhausted" {
		t.Fatalf("unexpected terminal result: %v %q %q", ok, msg, finalStatus)
	}
	if requests.Load() != 6 {
		t.Fatalf("platform got %d requests, want exactly 6", requests.Load())
	}
	if len(waits) != 6 {
		t.Fatalf("got %d waits, want initial plus five retries", len(waits))
	}
	if waits[0] < time.Second || waits[0] > 15*time.Second {
		t.Fatalf("initial jitter out of range: %v", waits[0])
	}
	for i, delay := range waits[1:] {
		low, high := retryWindow(i + 1)
		if delay < low || delay > high {
			t.Fatalf("retry %d wait out of range: %v", i+1, delay)
		}
	}
	for _, path := range []string{data, zip} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("failed upload must retain %s: %v", path, err)
		}
	}
}
func TestRetriesStopAfterSuccess(t *testing.T) {
	u := New(Config{MaxRetries: 5, LogDir: t.TempDir()})
	attempts := 0
	waits := 0
	exhausted := false
	ok, _ := u.runUpload("task", func(_, status, _ string) {
		if status == "retry_exhausted" {
			exhausted = true
		}
	}, nil,
		func(retry int) (bool, string, string) { attempts++; return retry == 1, "result", "login_failed" },
		func(time.Duration) bool { waits++; return false })
	if !ok || attempts != 2 || waits != 2 || exhausted {
		t.Fatalf("success must stop retries: ok=%v attempts=%d waits=%d exhausted=%v", ok, attempts, waits, exhausted)
	}
}
func TestServerSkipStopsPendingRetry(t *testing.T) {
	u := New(Config{MaxRetries: 5, LogDir: t.TempDir()})
	attempts := 0
	waits := 0
	u.runUpload("task", nil, nil, func(int) (bool, string, string) { attempts++; return false, "failed", "login_failed" }, func(time.Duration) bool { waits++; return waits == 2 })
	if attempts != 1 || waits != 2 {
		t.Fatalf("skip must prevent next request: attempts=%d waits=%d", attempts, waits)
	}
}
func TestBackoffCappedAndJittered(t *testing.T) {
	expected := []time.Duration{30, 60, 120, 240, 480, 600, 600}
	for i, n := range expected {
		low, high := retryWindow(i + 1)
		if high != n*time.Second || low != high/2 {
			t.Fatalf("invalid retry window %d: %v..%v", i+1, low, high)
		}
	}
	unique := map[time.Duration]bool{}
	for i := 0; i < 100; i++ {
		low, high := retryWindow(20)
		delay := randomDuration(low, high)
		if delay < 5*time.Minute || delay > 10*time.Minute {
			t.Fatalf("wait out of capped range: %v", delay)
		}
		unique[delay] = true
	}
	if len(unique) < 2 {
		t.Fatal("random staggering produced identical waits")
	}
}
