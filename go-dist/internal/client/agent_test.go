package client

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLocalControlRejectsUntrustedOriginsAndHosts(t *testing.T) {
	a, err := NewAgent(AgentOptions{Options: Options{Dest: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.logFile.Close()
	handler := a.Handler(18080)
	call := func(method, host, origin, contentType, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://"+host+path, strings.NewReader("{}"))
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		req.Header.Set("Content-Type", contentType)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		return resp
	}
	if resp := call("GET", "127.0.0.1:18080", "", "", "/api/local/status"); resp.Code != 200 {
		t.Fatal("local status unavailable")
	}
	if resp := call("POST", "127.0.0.1:18080", "https://evil.example", "application/json", "/api/local/start"); resp.Code != 403 {
		t.Fatal("external page can start local work")
	}
	if resp := call("GET", "evil.example:18080", "", "", "/api/local/status"); resp.Code != 403 {
		t.Fatal("DNS rebinding host accepted")
	}
	if resp := call("POST", "127.0.0.1:18080", "", "text/plain", "/api/local/pause"); resp.Code != 415 {
		t.Fatal("form mutation accepted")
	}
	a.mu.Lock()
	a.state.Server = "http://10.0.0.2:8000"
	a.mu.Unlock()
	if resp := call("OPTIONS", "127.0.0.1:18080", "http://10.0.0.2:8000", "", "/api/local/start"); resp.Code != 204 || resp.Header().Get("Access-Control-Allow-Origin") != "http://10.0.0.2:8000" {
		t.Fatal("connected server workspace cannot reach local agent")
	}
	if resp := call("GET", "127.0.0.1:18080", "", "", "/assets/workspace/app.js"); resp.Code != 200 {
		t.Fatal("workspace script unavailable")
	}
	for i := 0; i < 505; i++ {
		a.addLog(fmt.Sprint(i))
	}
	snapshot := a.Snapshot()
	if len(snapshot.Logs) != 500 || snapshot.Logs[0].Message != "5" {
		t.Fatal("log bound failed")
	}
	snapshot.Logs[0].Message = "changed"
	if a.Snapshot().Logs[0].Message == "changed" {
		t.Fatal("snapshot exposes mutable log state")
	}
}
func TestAgentPauseHelper(t *testing.T) {
	if os.Getenv("FENPEI_PAUSE_HELPER") == "" {
		return
	}
	path := os.Getenv("FENPEI_COUNTER_PATH")
	for n := 0; ; n++ {
		os.WriteFile(path, []byte(strconv.Itoa(n)), 0600)
		time.Sleep(15 * time.Millisecond)
	}
}
func TestImmediatePauseAndResumePreserveProcess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(map[string]bool{"ok": true}) }))
	defer server.Close()
	a, err := NewAgent(AgentOptions{Options: Options{Dest: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.logFile.Close()
	counter := filepath.Join(t.TempDir(), "counter")
	cmd := exec.Command(os.Args[0], "-test.run=^TestAgentPauseHelper$")
	cmd.Env = append(os.Environ(), "FENPEI_PAUSE_HELPER=1", "FENPEI_COUNTER_PATH="+counter)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	finished := make(chan struct{})
	if err := startWorkerProcess(cmd, finished); err != nil {
		t.Fatal(err)
	}
	defer func() { syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); cmd.Wait() }()
	a.mu.Lock()
	a.command = cmd
	a.state.Running = true
	a.state.Connected = true
	a.state.Server = server.URL
	a.mu.Unlock()
	read := func() int { data, _ := os.ReadFile(counter); n, _ := strconv.Atoi(string(data)); return n }
	deadline := time.Now().Add(2 * time.Second)
	for read() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if read() < 3 {
		t.Fatal("helper not working")
	}
	pid := cmd.Process.Pid
	if err := a.Pause(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		if strings.Contains(string(data), "State:\tT") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	pausedCounter := read()
	time.Sleep(100 * time.Millisecond)
	if read() != pausedCounter || !a.Snapshot().Paused {
		t.Fatal("work continues after immediate pause")
	}
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for read() <= pausedCounter && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if read() <= pausedCounter || a.Snapshot().Paused || a.command.Process.Pid != pid {
		t.Fatal("resume did not continue same worker process")
	}
}
