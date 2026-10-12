package uploader

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNotificationFailureDoesNotRepeatCompletedSteps(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	var puts atomic.Int32
	var uploads atomic.Int32
	minio := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			puts.Add(1)
			io.Copy(io.Discard, r.Body)
		}
		w.WriteHeader(200)
	}))
	defer minio.Close()
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/cab/platform/user/login":
			json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "retData": map[string]any{"token": "token", "tokenType": "Bearer"}})
		case "/embodied/collection-tasks/list":
			json.NewEncoder(w).Encode(map[string]any{"retCode": 200, "retData": map[string]any{"pages": 1, "records": []any{map[string]any{"id": "task1", "collectionTaskName": "JZ08/不同名称/20261010", "status": "pending"}}}})
		case "/embodied/collection-tasks/task1":
			json.NewEncoder(w).Encode(map[string]any{"retCode": 200, "retData": map[string]any{"remotePath": "/JZ08-demo-20261010.zip"}})
		case "/embodied/dag-client/info":
			json.NewEncoder(w).Encode(map[string]any{"retData": map[string]any{"oss": map[string]any{"bucket": "bucket", "accessKey": "key", "secretKey": "secret", "path": "task_data"}}})
		case "/embodied/collection-tasks/task1/upload":
			if uploads.Add(1) == 1 {
				// 平台收到了通知，但回复没有到客户端，模拟网站响应丢失。
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				connection.Close()
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"retCode": 200})
		default:
			json.NewEncoder(w).Encode(map[string]any{"retCode": 200})
		}
	}))
	defer platform.Close()
	t.Setenv("FENPEI_PLATFORM_PASSWORD", "test-password")
	t.Setenv("FENPEI_DAG_ACCESS_TOKEN", "test-token")
	t.Setenv("FENPEI_BASE_URL", platform.URL)
	t.Setenv("MINIO_ADDRESS", strings.TrimPrefix(minio.URL, "http://"))
	parent := t.TempDir()
	folder := filepath.Join(parent, "JZ08-demo-20261010")
	os.Mkdir(folder, 0755)
	os.WriteFile(filepath.Join(folder, "data.txt"), []byte("data"), 0644)
	output := t.TempDir()
	u := New(Config{OutputDir: output, LogDir: t.TempDir()})
	step := ""
	if ok, _ := u.doUpload(folder, "JZ08-demo-20261010", nil, nil, 0, &step); ok || step != "state_notify_failed" {
		t.Fatalf("expected notification failure, step=%s", step)
	}
	if _, err := os.Stat(folder); err != nil {
		t.Fatal("data must be retained while notification is unconfirmed")
	}
	if _, err := os.Stat(filepath.Join(output, "JZ08-demo-20261010.zip")); err != nil {
		t.Fatal("ZIP must be retained while notification is unconfirmed")
	}
	if ok, msg := u.doUpload(folder, "JZ08-demo-20261010", nil, nil, 1, &step); !ok {
		t.Fatalf("notification retry failed: %s", msg)
	}
	if puts.Load() != 1 || uploads.Load() != 2 {
		t.Fatalf("wanted one ZIP upload and two notifications, got %d and %d", puts.Load(), uploads.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, path := range []string{"/cab/platform/user/login", "/embodied/collection-tasks/list", "/embodied/collection-tasks/task1/start", "/embodied/collection-tasks/task1/complete", "/embodied/collection-tasks/task1", "/embodied/dag-client/info"} {
		if calls[path] != 1 {
			t.Fatalf("successful step repeated: %s called %d times", path, calls[path])
		}
	}
	if calls["/embodied/robots/health"] != 2 {
		t.Fatalf("expected required running/idle heartbeats only; got %d", calls["/embodied/robots/health"])
	}
	if _, err := os.Stat(folder); !os.IsNotExist(err) {
		t.Fatal("confirmed success should clean client folder")
	}
}

func TestTaskLookupRetryReusesSuccessfulLogin(t *testing.T) {
	var logins, lookups atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cab/platform/user/login" {
			logins.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "retData": map[string]any{"token": "token", "tokenType": "Bearer"}})
			return
		}
		lookups.Add(1)
		http.Error(w, "temporarily unavailable", 503)
	}))
	defer platform.Close()
	t.Setenv("FENPEI_PLATFORM_PASSWORD", "test-password")
	t.Setenv("FENPEI_DAG_ACCESS_TOKEN", "test-token")
	t.Setenv("FENPEI_BASE_URL", platform.URL)
	u := New(Config{LogDir: t.TempDir()})
	folder := t.TempDir()
	step := ""
	for retry := 0; retry < 3; retry++ {
		if ok, _ := u.doUpload(folder, "JZ08-demo-20261010", nil, nil, retry, &step); ok {
			t.Fatal("expected failed lookup")
		}
	}
	if logins.Load() != 1 || lookups.Load() != 3 {
		t.Fatalf("expected reused login; logins=%d lookups=%d", logins.Load(), lookups.Load())
	}
}

func TestUnauthorizedRequestInvalidatesOnlyLogin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer server.Close()
	p := NewPlatform()
	p.baseURL = server.URL
	p.token = "expired"
	p.username = "account"
	if _, err := p.call("GET", "/test", nil, nil); err == nil {
		t.Fatal("expected authorization failure")
	}
	if p.token != "" || p.username != "" {
		t.Fatal("expired login must be invalidated")
	}
}
