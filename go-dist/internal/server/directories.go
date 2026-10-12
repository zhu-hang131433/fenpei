package server

import (
	"net/http"
	"os"
	"path/filepath"
	"sort"
)

type directoryEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// handleDirectories 只列出源电脑上的文件夹，不读取文件内容。
func (s *Server) handleDirectories(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	home, _ := os.UserHomeDir()
	if path == "" {
		path = "/media"
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			path = home
		}
		if path == "" {
			path = "/"
		}
	}
	if !filepath.IsAbs(path) {
		writeJSON(w, 400, map[string]string{"error": "请选择绝对目录路径"})
		return
	}
	path = filepath.Clean(path)
	entries, err := os.ReadDir(path)
	if err != nil {
		message := "目录不存在或暂时不可读取"
		if os.IsPermission(err) {
			message = "服务端没有读取该目录的权限"
		}
		writeJSON(w, 400, map[string]string{"error": message})
		return
	}
	directories := make([]directoryEntry, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			directories = append(directories, directoryEntry{Name: entry.Name(), Path: filepath.Join(path, entry.Name())})
		}
	}
	sort.Slice(directories, func(i, j int) bool { return directories[i].Name < directories[j].Name })
	shortcuts := []directoryEntry{{Name: "移动硬盘", Path: "/media"}, {Name: "挂载目录", Path: "/mnt"}, {Name: "根目录", Path: "/"}}
	if home != "" {
		shortcuts = append([]directoryEntry{{Name: "主目录", Path: home}}, shortcuts...)
	}
	writeJSON(w, 200, map[string]any{"path": path, "parent": filepath.Dir(path), "directories": directories, "shortcuts": shortcuts})
}
