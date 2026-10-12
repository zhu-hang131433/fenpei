// Package workspace 嵌入独立客户端页面，供本机客户端与操作员入口共用。
package workspace

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed web
var files embed.FS

func Page(w http.ResponseWriter, r *http.Request, local bool) {
	data, err := files.ReadFile("web/index.html")
	if err != nil {
		http.Error(w, "页面不可用", 500)
		return
	}
	base := "http://127.0.0.1:18080"
	if local {
		base = ""
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write([]byte(strings.ReplaceAll(string(data), "{{AGENT_BASE}}", base)))
}
func Assets() http.Handler {
	sub, _ := fs.Sub(files, "web")
	return http.StripPrefix("/assets/workspace/", http.FileServer(http.FS(sub)))
}
