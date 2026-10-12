package server

import (
	"net/http"
	"time"

	"fenpei/internal/protocol"
)

// folderBusyLocked 在持有状态锁时判断任务是否仍由客户端处理。
func (s *Server) folderBusyLocked(folder string) bool {
	if f := s.st.Folders[folder]; f != nil && f.Status == protocol.StatusInProgress {
		return true
	}
	for _, worker := range s.st.Workers {
		if worker.ActiveFolder != folder && worker.CurrentFolder != folder {
			continue
		}
		last, err := time.ParseInLocation(protocol.TimeFormat, worker.LastSeen, time.Local)
		if err != nil || time.Since(last) < claimTimeoutSec*time.Second {
			return true
		}
	}
	return false
}

// handleRemoveFolder 只移除任务池记录，原始文件及平台数据不受影响。
func (s *Server) handleRemoveFolder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Folder string `json:"folder"`
	}
	if !readJSON(r, &req) || req.Folder == "" {
		writeJSON(w, 400, map[string]string{"error": "请选择要移除的文件夹"})
		return
	}
	s.scanMu.Lock()
	defer s.scanMu.Unlock()
	s.mu.Lock()
	if s.st.Folders[req.Folder] == nil {
		s.mu.Unlock()
		writeJSON(w, 404, map[string]string{"error": "该文件夹已不在数据池中"})
		return
	}
	if s.folderBusyLocked(req.Folder) {
		s.mu.Unlock()
		writeJSON(w, 409, map[string]string{"error": "任务仍被客户端领取或处理，请等任务结束后再删除"})
		return
	}
	sourceID := s.st.Folders[req.Folder].SourceID
	delete(s.st.Folders, req.Folder)
	if !s.sourceHasFoldersLocked(sourceID) {
		delete(s.st.Sources, sourceID)
	}
	for _, worker := range s.st.Workers {
		kept := worker.CompletedFolders[:0]
		for _, folder := range worker.CompletedFolders {
			if folder != req.Folder {
				kept = append(kept, folder)
			}
		}
		worker.CompletedFolders = kept
	}
	for key, entry := range s.st.UploadStatus {
		if entry.Folder == req.Folder {
			delete(s.st.UploadStatus, key)
		}
	}
	s.markDirty()
	s.mu.Unlock()
	s.pushEvent("remove", "从数据池移除 "+req.Folder)
	writeJSON(w, 200, map[string]bool{"ok": true})
}
