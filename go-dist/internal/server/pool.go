package server

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"fenpei/internal/protocol"
)

// Source 描述一个用户加入的挂载目录；ID 与路径分开，任务名称保持原样。
type Source struct {
	ID      string `json:"id"`
	Path    string `json:"path"`
	Label   string `json:"label"`
	AddedAt string `json:"added_at"`
	Device  uint64 `json:"device,omitempty"`
}
type SourceView struct {
	Source
	Available   bool `json:"available"`
	FolderCount int  `json:"folder_count"`
	Pending     int  `json:"pending"`
	Completed   int  `json:"completed"`
}
type scannedFolder struct {
	Name      string `json:"name"`
	FileCount int    `json:"file_count"`
	TotalSize int64  `json:"total_size"`
	Existing  bool   `json:"existing"`
	files     []protocol.FileInfo
}
type scanResult struct {
	Path    string          `json:"path"`
	Label   string          `json:"label"`
	Folders []scannedFolder `json:"folders"`
	Device  uint64          `json:"-"`
}
type sourceRequest struct {
	Path  string `json:"path"`
	Label string `json:"label"`
}
type addResult struct {
	SourceID string `json:"source_id"`
	Added    int    `json:"added"`
	Existing int    `json:"existing"`
}

func newSourceID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "src-" + hex.EncodeToString(b)
}
func deviceOf(path string) uint64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Dev)
	}
	return 0
}
func sourceAvailable(src *Source) bool {
	if src == nil {
		return false
	}
	info, err := os.Stat(src.Path)
	return err == nil && info.IsDir() && (src.Device == 0 || deviceOf(src.Path) == src.Device)
}
func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && !filepath.IsAbs(rel)
}
func (s *Server) folderAvailable(f *Folder) bool {
	src := s.st.Sources[f.SourceID]
	if !sourceAvailable(src) {
		return false
	}
	info, err := os.Stat(filepath.Join(src.Path, f.Name))
	return err == nil && info.IsDir()
}
func (s *Server) sourceViewsLocked() ([]SourceView, map[string]bool) {
	views := make([]SourceView, 0, len(s.st.Sources))
	available := map[string]bool{}
	for id, src := range s.st.Sources {
		v := SourceView{Source: *src, Available: sourceAvailable(src)}
		available[id] = v.Available
		for _, f := range s.st.Folders {
			if f.SourceID == id {
				v.FolderCount++
				if f.Status == protocol.StatusCompleted {
					v.Completed++
				}
				if f.Status == protocol.StatusPending && !f.Skip {
					v.Pending++
				}
			}
		}
		views = append(views, v)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Path < views[j].Path })
	return views, available
}

// 扫描时不持有业务锁；遇到不可读文件拒绝整个导入，不留下部分任务。
func scanPoolDirectory(path, label string) (*scanResult, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("请输入源电脑上的绝对目录路径，例如 /media/用户名/硬盘名")
	}
	root, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("目录不可访问：%w", err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("路径不是可读取的目录")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("读取目录失败：%w", err)
	}
	if strings.TrimSpace(label) == "" {
		label = filepath.Base(root)
	}
	result := &scanResult{Path: root, Label: strings.TrimSpace(label), Folders: []scannedFolder{}, Device: deviceOf(root)}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		folder := scannedFolder{Name: entry.Name(), files: []protocol.FileInfo{}}
		err = filepath.WalkDir(filepath.Join(root, entry.Name()), func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			// 不遍历符号链接，避免导入目录外的数据。
			if d.Type()&os.ModeSymlink != 0 {
				return nil
			}
			if d.IsDir() {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			hash := sha256.New()
			copied, copyErr := io.Copy(hash, f)
			after, statErr := f.Stat()
			f.Close()
			if copyErr != nil {
				return copyErr
			}
			if statErr != nil {
				return statErr
			}
			if copied != info.Size() || after.Size() != info.Size() || !after.ModTime().Equal(info.ModTime()) {
				return fmt.Errorf("文件在扫描期间发生变化：%s", path)
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			folder.files = append(folder.files, protocol.FileInfo{Path: filepath.ToSlash(rel), Size: info.Size(), SHA256: hex.EncodeToString(hash.Sum(nil))})
			folder.TotalSize += info.Size()
			folder.FileCount++
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("扫描 %s 失败，未导入任何任务：%w", entry.Name(), err)
		}
		if folder.FileCount > 0 {
			result.Folders = append(result.Folders, folder)
		}
	}
	if result.Device != deviceOf(root) {
		return nil, fmt.Errorf("扫描期间数据来源发生变化，请重新扫描")
	}
	return result, nil
}
func (s *Server) sourceHasFoldersLocked(id string) bool {
	for _, f := range s.st.Folders {
		if f.SourceID == id {
			return true
		}
	}
	return false
}
func (s *Server) validateRootLocked(root string) error {
	for id, src := range s.st.Sources {
		if !s.sourceHasFoldersLocked(id) {
			continue
		}
		if src.Path != root && (within(src.Path, root) || within(root, src.Path)) {
			return fmt.Errorf("目录与已加入来源 %s 存在包含关系，请选择不重叠的目录", src.Path)
		}
	}
	return nil
}
func (s *Server) addSource(path, label string) (*addResult, error) {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()
	scanned, err := scanPoolDirectory(path, label)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.validateRootLocked(scanned.Path); err != nil {
		return nil, err
	}
	var src *Source
	for _, existing := range s.st.Sources {
		if existing.Path == scanned.Path {
			src = existing
			break
		}
	}
	if src == nil {
		if len(scanned.Folders) == 0 {
			return nil, fmt.Errorf("未发现包含文件的一级子文件夹，请选择数据目录的上一级")
		}
		src = &Source{ID: newSourceID(), Path: scanned.Path, Label: scanned.Label, AddedAt: protocol.Now(), Device: scanned.Device}
		s.st.Sources[src.ID] = src
	} else {
		if src.Device != 0 && scanned.Device != src.Device && s.sourceHasFoldersLocked(src.ID) {
			return nil, fmt.Errorf("该路径的设备已变化，请先移除旧来源的全部任务，再加入新硬盘")
		}
		src.Device = scanned.Device
		if strings.TrimSpace(label) != "" {
			src.Label = scanned.Label
		}
	}
	result := &addResult{SourceID: src.ID}
	existingNames := map[string]bool{}
	for _, f := range s.st.Folders {
		if f.SourceID == src.ID {
			existingNames[f.Name] = true
		}
	}
	for _, entry := range scanned.Folders {
		if existingNames[entry.Name] {
			result.Existing++
			continue
		}
		key := src.ID + "/" + entry.Name
		files := make([]protocol.FileInfo, 0, len(entry.files))
		for _, file := range entry.files {
			files = append(files, protocol.FileInfo{Path: src.ID + "/" + file.Path, Size: file.Size, SHA256: file.SHA256})
		}
		s.st.Folders[key] = &Folder{Name: entry.Name, SourceID: src.ID, Files: files, FileCount: entry.FileCount, TotalSize: entry.TotalSize, Status: protocol.StatusPending}
		result.Added++
	}
	s.markDirty()
	s.pushEvent("source", fmt.Sprintf("来源 %s：新增 %d 个文件夹，已有 %d 个", src.Label, result.Added, result.Existing))
	return result, nil
}
func (s *Server) handleSourceScan(w http.ResponseWriter, r *http.Request) {
	var req sourceRequest
	if !readJSON(r, &req) {
		writeJSON(w, 400, map[string]string{"error": "请求格式无效"})
		return
	}
	scanned, err := scanPoolDirectory(req.Path, req.Label)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	s.mu.RLock()
	err = s.validateRootLocked(scanned.Path)
	for _, src := range s.st.Sources {
		if src.Path == scanned.Path {
			for i := range scanned.Folders {
				for _, f := range s.st.Folders {
					if f.SourceID == src.ID && f.Name == scanned.Folders[i].Name {
						scanned.Folders[i].Existing = true
						break
					}
				}
			}
		}
	}
	s.mu.RUnlock()
	if err != nil {
		writeJSON(w, 409, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, scanned)
}
func (s *Server) handleSourceAdd(w http.ResponseWriter, r *http.Request) {
	var req sourceRequest
	if !readJSON(r, &req) {
		writeJSON(w, 400, map[string]string{"error": "请求格式无效"})
		return
	}
	result, err := s.addSource(req.Path, req.Label)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, result)
}

// 只提供池子文件清单中的文件，不暴露任意来源文件；兼容旧任务路径。
func (s *Server) resolvePoolFile(path string) (string, error) {
	if path == "" || strings.Contains(path, "\\") || filepath.IsAbs(path) || filepath.ToSlash(filepath.Clean(path)) != path {
		return "", fmt.Errorf("无效文件路径")
	}
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("文件不在任务池中")
	}
	key := parts[0]
	s.mu.RLock()
	if s.st.Folders[key] == nil && len(parts) >= 3 {
		key = parts[0] + "/" + parts[1]
	}
	f := s.st.Folders[key]
	if f == nil {
		s.mu.RUnlock()
		return "", fmt.Errorf("文件不在任务池中")
	}
	src := s.st.Sources[f.SourceID]
	if !sourceAvailable(src) {
		s.mu.RUnlock()
		return "", fmt.Errorf("数据来源不可用，请重新连接硬盘")
	}
	known := false
	for _, file := range f.Files {
		if file.Path == path {
			known = true
			break
		}
	}
	root := src.Path
	relative := path
	if strings.HasPrefix(key, src.ID+"/") {
		relative = strings.TrimPrefix(path, src.ID+"/")
	}
	s.mu.RUnlock()
	if !known {
		return "", fmt.Errorf("文件不在任务清单中")
	}
	full, err := filepath.EvalSymlinks(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil || !within(root, full) {
		return "", fmt.Errorf("文件不可用或超出来源目录")
	}
	return full, nil
}
