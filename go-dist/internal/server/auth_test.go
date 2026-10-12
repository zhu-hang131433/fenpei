package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebRoleRoutingAndPermissions(t *testing.T) {
	t.Setenv("FENPEI_ADMIN_USER", "manager")
	t.Setenv("FENPEI_ADMIN_PASSWORD", "admin-test-password")
	t.Setenv("FENPEI_OPERATOR_USER", "worker")
	t.Setenv("FENPEI_OPERATOR_PASSWORD", "operator-test-password")
	s := queueTestServer(t)
	handler := s.Handler()
	call := func(method, path, body string, cookie *http.Cookie, origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if cookie != nil {
			req.AddCookie(cookie)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response
	}
	for _, path := range []string{"/api/status", "/api/directories", "/api/events/list", "/api/auth/me"} {
		if response := call("GET", path, "", nil, ""); response.Code != 401 {
			t.Fatalf("anonymous %s: %d", path, response.Code)
		}
	}
	if response := call("GET", "/server", "", nil, ""); response.Code != 303 || response.Header().Get("Location") != "/login" {
		t.Fatal("anonymous page must redirect to login")
	}
	if response := call("POST", "/api/auth/login", `{"username":"manager","password":"wrong"}`, nil, ""); response.Code != 401 {
		t.Fatal("wrong password accepted")
	}
	if response := call("POST", "/api/auth/login", `{"username":"manager","password":"admin-test-password"}`, nil, "https://untrusted.example"); response.Code != 403 {
		t.Fatal("cross origin login accepted")
	}
	for _, entry := range []struct{ username, password, redirect string }{{"manager", "admin-test-password", "/server"}, {"worker", "operator-test-password", "/workspace"}} {
		response := call("POST", "/api/auth/login", `{"username":"`+entry.username+`","password":"`+entry.password+`"}`, nil, "")
		if response.Code != 200 || !strings.Contains(response.Body.String(), entry.redirect) {
			t.Fatalf("login failed: %s", response.Body.String())
		}
		cookies := response.Result().Cookies()
		if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
			t.Fatal("session cookie flags missing")
		}
		cookie := cookies[0]
		if response = call("GET", "/", "", cookie, ""); response.Header().Get("Location") != entry.redirect {
			t.Fatal("incorrect role destination")
		}
		if response = call("GET", entry.redirect, "", cookie, ""); response.Code != 200 {
			t.Fatal("role page unavailable")
		}
		if entry.username == "worker" {
			if response = call("GET", "/api/status", "", cookie, ""); response.Code != 403 {
				t.Fatal("operator can read administrative data")
			}
			if response = call("POST", "/api/pause", `{"paused":true}`, cookie, ""); response.Code != 403 {
				t.Fatal("operator can pause dispatch")
			}
			if response = call("GET", "/server", "", cookie, ""); response.Header().Get("Location") != "/workspace" {
				t.Fatal("operator routed to admin page")
			}
		} else {
			if response = call("GET", "/api/status", "", cookie, ""); response.Code != 200 {
				t.Fatal("admin status unavailable")
			}
			if response = call("POST", "/api/pause", `{"paused":true}`, cookie, "https://untrusted.example"); response.Code != 403 {
				t.Fatal("cross origin mutation accepted")
			}
			if response = call("POST", "/api/pause", `{"paused":true}`, cookie, ""); response.Code != 200 || !s.snapshot().Paused {
				t.Fatal("admin pause failed")
			}
		}
		if response = call("POST", "/api/auth/logout", `{}`, cookie, ""); response.Code != 200 {
			t.Fatal("logout failed")
		}
		if response = call("GET", "/api/auth/me", "", cookie, ""); response.Code != 401 {
			t.Fatal("logged out session remains valid")
		}
	}
	for _, path := range []string{"/assets/server/app.js", "/assets/server/view.js", "/assets/server/style.css", "/assets/js/api.js", "/assets/auth.css"} {
		if response := call("GET", path, "", nil, ""); response.Code != 200 {
			t.Fatalf("asset %s unavailable: %d", path, response.Code)
		}
	}
}

func TestLoginAttemptLimit(t *testing.T) {
	t.Setenv("FENPEI_ADMIN_PASSWORD", "test-password")
	auth := newWebAuth()
	for i := 0; i < 11; i++ {
		req := httptest.NewRequest("POST", "/api/auth/login", strings.NewReader(`{"username":"admin","password":"incorrect"}`))
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		auth.login(response, req)
		expected := 401
		if i == 10 {
			expected = 429
		}
		if response.Code != expected {
			t.Fatalf("attempt %d: %d", i, response.Code)
		}
	}
}

func TestUnifiedApplicationRoles(t *testing.T) {
	t.Setenv("FENPEI_ADMIN_USER", "")
	t.Setenv("FENPEI_ADMIN_PASSWORD", "")
	t.Setenv("FENPEI_OPERATOR_USER", "")
	t.Setenv("FENPEI_OPERATOR_PASSWORD", "")
	s := queueTestServer(t)
	var roles []string
	h := s.ApplicationHandler(func(role string) error { roles = append(roles, role); return nil }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, username := range []string{"admin", "pc01"} {
		req := httptest.NewRequest("POST", "/api/auth/login", strings.NewReader(`{"username":"`+username+`","password":"root"}`))
		req.Header.Set("Content-Type", "application/json")
		out := httptest.NewRecorder()
		h.ServeHTTP(out, req)
		if out.Code != 200 {
			t.Fatalf("%s: %s", username, out.Body.String())
		}
		cookie := out.Result().Cookies()[0]
		req = httptest.NewRequest("GET", "/api/local/status", nil)
		req.AddCookie(cookie)
		out = httptest.NewRecorder()
		h.ServeHTTP(out, req)
		want := 403
		if username == "pc01" {
			want = 204
		}
		if out.Code != want {
			t.Fatalf("%s local API: %d", username, out.Code)
		}
		if username == "pc01" {
			req = httptest.NewRequest("GET", "/workspace", nil)
			req.AddCookie(cookie)
			out = httptest.NewRecorder()
			h.ServeHTTP(out, req)
			if !strings.Contains(out.Body.String(), `data-agent-base=""`) {
				t.Fatal("unified client must use same-origin local API")
			}
		}
	}
	if strings.Join(roles, ",") != "admin,operator" {
		t.Fatalf("activation roles: %v", roles)
	}
}
