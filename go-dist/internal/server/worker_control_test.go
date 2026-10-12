package server

import (
	"context"
	"fenpei/internal/protocol"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPausedWorkerLeavesQueueAndDoesNotClaim(t *testing.T) {
	t.Setenv("FENPEI_ADMIN_PASSWORD", "root")
	s := queueTestServer(t)
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()
	id, _ := s.register("127.0.0.1", "pause-worker")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan protocol.NextResp, 1)
	go func() { task, _, _ := pollTask(ctx, httpServer.URL, id); done <- task }()
	waitQueue(t, s, 1)
	control := func(paused bool) {
		t.Helper()
		err := protocol.JSONRequest("POST", httpServer.URL+"/api/worker/pause", protocol.PauseReq{Paused: paused}, map[string]string{"X-Worker-Id": id}, time.Second, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	control(true)
	select {
	case task := <-done:
		if !task.Waiting {
			t.Fatal("paused request must return to waiting")
		}
	case <-time.After(time.Second):
		t.Fatal("paused worker blocked queue")
	}
	waitQueue(t, s, 0)
	root := t.TempDir()
	os.Mkdir(filepath.Join(root, "A"), 0755)
	os.WriteFile(filepath.Join(root, "A", "data.txt"), []byte("data"), 0644)
	if _, err := s.addSource(root, "test"); err != nil {
		t.Fatal(err)
	}
	if name, _ := s.next(id); name != "" {
		t.Fatal("paused worker claimed a new task")
	}
	if !s.snapshot().Workers[id].Paused {
		t.Fatal("pause not visible in monitor")
	}
	control(false)
	name, folder := s.next(id)
	if name == "" || folder == nil {
		t.Fatal("resume failed to claim")
	}
	control(true)
	other, _ := s.register("127.0.0.2", "other-worker")
	if name, _ := s.next(other); name != "" {
		t.Fatal("active paused task reassigned")
	}
	var response protocol.SimpleResp
	if err := protocol.JSONRequest("POST", httpServer.URL+"/api/worker/pause", protocol.PauseReq{Paused: true}, map[string]string{"X-Worker-Id": "unknown"}, time.Second, &response); err == nil {
		t.Fatal("unknown worker can pause")
	}
}
