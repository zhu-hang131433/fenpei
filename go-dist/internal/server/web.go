package server

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// 前端静态资源编译期打包进二进制，部署无需额外文件。
//
//go:embed web
var webFS embed.FS

func serveWebFile(w http.ResponseWriter, r *http.Request, path string) {
	b, err := webFS.ReadFile(path)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(b)
}

func (s *Server) serveIndex() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { serveWebFile(w, r, "web/server/index.html") }
}

// webAssets 提供 /assets/ 静态资源
func webAssets() http.Handler {
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/assets/")
		w.Header().Set("Cache-Control", "no-cache")
		fileServer.ServeHTTP(w, r)
	})
}
