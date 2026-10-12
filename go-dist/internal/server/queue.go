package server

import (
	"context"
	"net/http"
	"time"

	"fenpei/internal/protocol"
)

const longPollDuration = 45 * time.Second

type taskWaiter struct {
	workerID string
	ctx      context.Context
	ready    chan struct{}
}

func (s *Server) notifyQueue() {
	select {
	case s.queueWake <- struct{}{}:
	default:
	}
}

// 服务端集中检查来源恢复和领取超时，不要求空闲客户端短周期轮询。
func (s *Server) queueLoop() {
	defer close(s.queueDone)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopping:
			return
		case <-s.queueWake:
		case <-ticker.C:
		}
		s.queueMu.Lock()
		if len(s.waiters) > 0 {
			select {
			case s.waiters[0].ready <- struct{}{}:
			default:
			}
		}
		s.queueMu.Unlock()
	}
}
func (s *Server) waitingWorkers() map[string]bool {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	waiting := map[string]bool{}
	for _, w := range s.waiters {
		if w.ctx.Err() == nil {
			waiting[w.workerID] = true
		}
	}
	return waiting
}
func (s *Server) removeWaiterLocked(target *taskWaiter) {
	for i, w := range s.waiters {
		if w == target {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			break
		}
	}
	s.notifyQueue()
}
func (s *Server) awaitTask(w http.ResponseWriter, r *http.Request, workerID string) {
	waiter := &taskWaiter{workerID: workerID, ctx: r.Context(), ready: make(chan struct{}, 1)}
	s.queueMu.Lock()
	for _, other := range s.waiters {
		if other.workerID == workerID && other.ctx.Err() == nil {
			s.queueMu.Unlock()
			writeJSON(w, 409, map[string]string{"error": "该客户端已有等待领取请求，请勿重复启动同一身份"})
			return
		}
	}
	s.mu.Lock()
	if worker := s.st.Workers[workerID]; worker != nil {
		worker.ActiveFolder = ""
	}
	s.mu.Unlock()
	s.waiters = append(s.waiters, waiter)
	s.queueMu.Unlock()
	s.notifyQueue()
	defer func() { s.queueMu.Lock(); s.removeWaiterLocked(waiter); s.queueMu.Unlock() }()
	timer := time.NewTimer(longPollDuration)
	defer timer.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.stopping:
			writeJSON(w, 503, map[string]string{"error": "服务正在停止，请重新连接"})
			return
		case <-timer.C:
			writeJSON(w, 200, protocol.NextResp{Waiting: true, Message: "长轮询到期，请重新加入等待队列"})
			return
		case <-waiter.ready:
			if s.workerPaused(workerID) {
				writeJSON(w, 200, protocol.NextResp{Waiting: true, Message: "客户端已暂停"})
				return
			}
			s.queueMu.Lock()
			if r.Context().Err() != nil {
				s.queueMu.Unlock()
				return
			}
			if len(s.waiters) == 0 || s.waiters[0] != waiter {
				s.queueMu.Unlock()
				continue
			}
			name, f := s.next(workerID)
			if name == "" {
				s.queueMu.Unlock()
				continue
			}
			s.mu.RLock()
			response := protocol.NextResp{ClaimToken: f.ClaimToken, Folder: name, FolderName: f.Name, FileCount: f.FileCount, TotalSize: f.TotalSize, Files: append([]protocol.FileInfo{}, f.Files...)}
			for _, folder := range s.st.Folders {
				if folder.TotalSize > response.MaxFolderSize {
					response.MaxFolderSize = folder.TotalSize
				}
			}
			s.mu.RUnlock()
			s.removeWaiterLocked(waiter)
			s.queueMu.Unlock()
			writeJSON(w, 200, response)
			return
		}
	}
}
