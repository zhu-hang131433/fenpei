package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fenpei/internal/protocol"
)

func queueTestServer(t *testing.T) *Server {
	t.Helper()
	s := New("")
	s.stateFile = filepath.Join(t.TempDir(), "state.json")
	s.logDir = t.TempDir()
	s.Start()
	t.Cleanup(s.Stop)
	return s
}
func waitQueue(t *testing.T, s *Server, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.queueMu.Lock()
		n := len(s.waiters)
		s.queueMu.Unlock()
		if n == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waiting queue did not reach %d", count)
}
func pollTask(ctx context.Context, url, id string) (protocol.NextResp, int, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", url+"/api/next", nil)
	req.Header.Set("X-Worker-Id", id)
	req.Header.Set("X-Pool-Protocol", "2")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return protocol.NextResp{}, 0, err
	}
	defer res.Body.Close()
	var task protocol.NextResp
	err = json.NewDecoder(res.Body).Decode(&task)
	return task, res.StatusCode, err
}
func TestWaitingClientsFIFOAndResume(t *testing.T) {
	s := queueTestServer(t)
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, _ := s.register("127.0.0.1", "first")
	second, _ := s.register("127.0.0.1", "second")
	results := make(chan protocol.NextResp, 2)
	go func() { task, _, _ := pollTask(ctx, httpServer.URL, first); results <- task }()
	waitQueue(t, s, 1)
	go func() { task, _, _ := pollTask(ctx, httpServer.URL, second); results <- task }()
	waitQueue(t, s, 2)
	s.setPaused(true)
	root := t.TempDir()
	for _, name := range []string{"A", "B"} {
		if err := os.Mkdir(filepath.Join(root, name), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name, "data.txt"), []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.addSource(root, "test"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-results:
		t.Fatal("paused server allocated a task")
	case <-time.After(30 * time.Millisecond):
	}
	started := time.Now()
	s.setPaused(false)
	for i := 0; i < 2; i++ {
		select {
		case task := <-results:
			if task.Folder == "" {
				t.Fatal("expected task")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("resume failed to wake clients")
		}
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("dispatch did not wake promptly")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, f := range s.st.Folders {
		expected := first
		if f.Name == "B" {
			expected = second
		}
		if f.ClaimedBy == nil || *f.ClaimedBy != expected {
			t.Fatalf("FIFO allocation failed for %s", f.Name)
		}
	}
}
func TestDisconnectedWaiterAndDuplicateRequest(t *testing.T) {
	s := queueTestServer(t)
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()
	id, _ := s.register("127.0.0.1", "client")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { pollTask(ctx, httpServer.URL, id); close(done) }()
	waitQueue(t, s, 1)
	_, status, err := pollTask(context.Background(), httpServer.URL, id)
	if err != nil || status != 409 {
		t.Fatalf("duplicate wait should be rejected: %d %v", status, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not cancel")
	}
	waitQueue(t, s, 0)
}
