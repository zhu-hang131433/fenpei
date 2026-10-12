package server

import (
	"fenpei/internal/protocol"
	"fmt"
	"net/http"
)

// 暂停只作用于这个客户端，不影响全局分发开关或其他设备。
func (s *Server) handleWorkerPause(w http.ResponseWriter, r *http.Request) {
	var req protocol.PauseReq
	if !readJSON(r, &req) {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	id := r.Header.Get("X-Worker-Id")
	s.mu.Lock()
	worker := s.st.Workers[id]
	if worker == nil {
		s.mu.Unlock()
		writeJSON(w, 401, map[string]string{"error": "unregistered worker"})
		return
	}
	changed := worker.Paused != req.Paused
	worker.Paused = req.Paused
	worker.LastSeen = protocol.Now()
	label := worker.Label
	s.mu.Unlock()
	s.queueMu.Lock()
	for _, waiter := range s.waiters {
		if waiter.workerID == id {
			select {
			case waiter.ready <- struct{}{}:
			default:
			}
		}
	}
	s.queueMu.Unlock()
	s.notifyQueue()
	if changed {
		action := "恢复工作"
		if req.Paused {
			action = "暂停工作"
		}
		s.pushEvent("worker", fmt.Sprintf("客户端 %s %s", label, action))
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}
func (s *Server) workerPaused(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st.Workers[id] != nil && s.st.Workers[id].Paused
}
