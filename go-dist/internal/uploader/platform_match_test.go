package uploader

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestDateMatchingAcrossPages(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "unique", true: "ambiguous"}[duplicate], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Current int `json:"current"`
				}
				json.NewDecoder(r.Body).Decode(&body)
				records := []any{map[string]any{"id": "one", "collectionTaskName": "名称完全不同/20261010"}}
				if body.Current == 2 {
					records = []any{map[string]any{"id": "two", "collectionTaskName": "其他任务/20261011"}}
					if duplicate {
						records = []any{map[string]any{"id": "two", "collectionTaskName": "另一个/20261010"}}
					}
				}
				json.NewEncoder(w).Encode(map[string]any{"retCode": 200, "retData": map[string]any{"pages": 2, "records": records}})
			}))
			defer server.Close()
			p := NewPlatform()
			p.baseURL = server.URL
			id, _, err := p.FindMatchingTask("JZ08-本地名称-20261010")
			if duplicate {
				if err == nil {
					t.Fatal("duplicate suffix must fail")
				}
			} else if err != nil || id != "one" {
				t.Fatalf("id=%s error=%v", id, err)
			}
		})
	}
	for _, name := range []string{"JZ08-demo", "20261010-任务", "任务120261010"} {
		if TaskDateSuffix(name) != "" {
			t.Fatalf("unexpected suffix: %s", name)
		}
	}
}

func TestTaskLifecycleBeforeUpload(t *testing.T) {
	for _, status := range []string{"pending", "in_progress", "collecting", "completed"} {
		t.Run(status, func(t *testing.T) {
			var steps []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/cab/platform/user/login":
					json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "retData": map[string]any{"token": "token"}})
				case "/embodied/collection-tasks/list":
					json.NewEncoder(w).Encode(map[string]any{"retCode": 200, "retData": map[string]any{"pages": 1, "records": []any{map[string]any{"id": "task", "collectionTaskName": "平台任务/20261010", "status": status}}}})
				case "/embodied/collection-tasks/task/start":
					steps = append(steps, "start")
					json.NewEncoder(w).Encode(map[string]any{"retCode": 200})
				case "/embodied/collection-tasks/task/complete":
					steps = append(steps, "complete")
					json.NewEncoder(w).Encode(map[string]any{"retCode": 200})
				case "/embodied/collection-tasks/task":
					steps = append(steps, "info")
					http.Error(w, "stop before upload", 503)
				default:
					json.NewEncoder(w).Encode(map[string]any{"retCode": 200})
				}
			}))
			defer server.Close()
			t.Setenv("FENPEI_BASE_URL", server.URL)
			u := New(Config{LogDir: t.TempDir()})
			step := ""
			u.doUpload(t.TempDir(), "JZ08-本地任务-20261010", nil, nil, 0, &step)
			want := []string{"info"}
			if status == "pending" {
				want = []string{"start", "complete", "info"}
			} else if status != "completed" {
				want = []string{"complete", "info"}
			}
			if !reflect.DeepEqual(steps, want) {
				t.Fatalf("steps=%v want=%v", steps, want)
			}
		})
	}
}
