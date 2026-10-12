package server

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fenpei/internal/workspace"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type webUser struct {
	Username string `json:"username"`
	Role     string `json:"role"`
}
type webSession struct {
	user    webUser
	expires time.Time
}
type loginAttempt struct {
	count int
	until time.Time
}
type webAuth struct {
	onLogin        func(string) error
	localWorkspace bool
	mu             sync.Mutex
	users          map[string]struct {
		password [32]byte
		role     string
	}
	sessions map[string]webSession
	attempts map[string]loginAttempt
}

func randomSecret() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func newWebAuth() *webAuth {
	a := &webAuth{users: make(map[string]struct {
		password [32]byte
		role     string
	}), sessions: make(map[string]webSession), attempts: make(map[string]loginAttempt)}
	admin := os.Getenv("FENPEI_ADMIN_USER")
	if admin == "" {
		admin = "admin"
	}
	password := os.Getenv("FENPEI_ADMIN_PASSWORD")
	if password == "" {
		password = "root"
		log.Printf("服务端管理员账号: %s；使用默认管理员密码（可通过 FENPEI_ADMIN_PASSWORD 覆盖）", admin)
	}
	a.users[admin] = struct {
		password [32]byte
		role     string
	}{sha256.Sum256([]byte(password)), "admin"}
	operator, operatorPassword := os.Getenv("FENPEI_OPERATOR_USER"), os.Getenv("FENPEI_OPERATOR_PASSWORD")
	if operator == "" && operatorPassword == "" {
		operator, operatorPassword = "pc01", "root"
	}
	if operator != "" && operatorPassword != "" && operator != admin {
		a.users[operator] = struct {
			password [32]byte
			role     string
		}{sha256.Sum256([]byte(operatorPassword)), "operator"}
	}
	return a
}
func destination(role string) string {
	if role == "admin" {
		return "/server"
	}
	return "/workspace"
}
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return err == nil && u.Scheme == scheme && u.Host == r.Host
}
func (a *webAuth) current(r *http.Request) (webUser, bool) {
	cookie, err := r.Cookie("fenpei_session")
	if err != nil {
		return webUser{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	session, ok := a.sessions[cookie.Value]
	if !ok || time.Now().After(session.expires) {
		delete(a.sessions, cookie.Value)
		return webUser{}, false
	}
	return session.user, true
}
func (a *webAuth) login(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r) || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		writeJSON(w, 403, map[string]string{"error": "请求来源无效"})
		return
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !readJSON(r, &req) {
		writeJSON(w, 400, map[string]string{"error": "请输入账号和密码"})
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	ip := remoteIP(r)
	for key, entry := range a.attempts {
		if now.After(entry.until) {
			delete(a.attempts, key)
		}
	}
	attempt := a.attempts[ip]
	if attempt.count >= 10 || len(a.attempts) >= 10000 {
		writeJSON(w, 429, map[string]string{"error": "登录尝试过多，请 5 分钟后重试"})
		return
	}
	user, found := a.users[req.Username]
	password := sha256.Sum256([]byte(req.Password))
	matches := subtle.ConstantTimeCompare(user.password[:], password[:]) == 1
	if !found || !matches {
		if attempt.count == 0 {
			attempt.until = now.Add(5 * time.Minute)
		}
		attempt.count++
		a.attempts[ip] = attempt
		writeJSON(w, 401, map[string]string{"error": "账号或密码不正确"})
		return
	}
	if a.onLogin != nil {
		if err := a.onLogin(user.role); err != nil {
			writeJSON(w, 503, map[string]string{"error": err.Error()})
			return
		}
	}
	delete(a.attempts, ip)
	for token, session := range a.sessions {
		if now.After(session.expires) {
			delete(a.sessions, token)
		}
	}
	if len(a.sessions) >= 1000 {
		writeJSON(w, 503, map[string]string{"error": "登录会话已满，请稍后重试"})
		return
	}
	token := randomSecret()
	a.sessions[token] = webSession{webUser{req.Username, user.role}, now.Add(12 * time.Hour)}
	http.SetCookie(w, &http.Cookie{Name: "fenpei_session", Value: token, Path: "/", HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode, MaxAge: 43200})
	writeJSON(w, 200, map[string]string{"role": user.role, "redirect": destination(user.role)})
}
func (a *webAuth) wrap(next http.Handler) http.Handler {
	management := map[string]bool{"/api/status": true, "/api/directories": true, "/api/sources/scan": true, "/api/sources": true, "/api/folders/remove": true, "/api/events": true, "/api/events/list": true, "/api/pause": true, "/api/skip": true, "/api/batch_skip": true, "/api/mark_uploaded": true}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		path := r.URL.Path
		if path == "/api/auth/login" && r.Method == "POST" {
			a.login(w, r)
			return
		}
		if path == "/login" && r.Method == "GET" {
			if user, ok := a.current(r); ok {
				http.Redirect(w, r, destination(user.Role), http.StatusSeeOther)
				return
			}
			serveWebFile(w, r, "web/login.html")
			return
		}
		if path == "/api/auth/logout" && r.Method == "POST" {
			if !sameOrigin(r) {
				writeJSON(w, 403, map[string]string{"error": "请求来源无效"})
				return
			}
			if cookie, err := r.Cookie("fenpei_session"); err == nil {
				a.mu.Lock()
				delete(a.sessions, cookie.Value)
				a.mu.Unlock()
			}
			http.SetCookie(w, &http.Cookie{Name: "fenpei_session", Path: "/", MaxAge: -1, HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode})
			writeJSON(w, 200, map[string]bool{"ok": true})
			return
		}
		protectedPage := path == "/" || path == "/status" || path == "/server" || path == "/server/" || path == "/workspace" || path == "/workspace/"
		localAPI := strings.HasPrefix(path, "/api/local/")
		if management[path] || protectedPage || path == "/api/auth/me" || localAPI {
			user, ok := a.current(r)
			if !ok {
				if protectedPage {
					http.Redirect(w, r, "/login", http.StatusSeeOther)
				} else {
					writeJSON(w, 401, map[string]string{"error": "请先登录"})
				}
				return
			}
			if path == "/api/auth/me" {
				writeJSON(w, 200, user)
				return
			}
			if localAPI {
				if user.Role != "operator" {
					writeJSON(w, 403, map[string]string{"error": "请使用客户端账号登录"})
					return
				}
				if r.Method != "GET" && !sameOrigin(r) {
					writeJSON(w, 403, map[string]string{"error": "请求来源无效"})
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			if protectedPage {
				if path == "/" || path == "/status" || (user.Role == "admin" && strings.HasPrefix(path, "/workspace")) || (user.Role != "admin" && strings.HasPrefix(path, "/server")) {
					http.Redirect(w, r, destination(user.Role), http.StatusSeeOther)
					return
				}
				if user.Role == "operator" {
					workspace.Page(w, r, a.localWorkspace)
					return
				}
			}
			if user.Role != "admin" {
				writeJSON(w, 403, map[string]string{"error": "仅管理员可访问服务端管理功能"})
				return
			}
			if r.Method != "GET" && !sameOrigin(r) {
				writeJSON(w, 403, map[string]string{"error": "请求来源无效"})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
