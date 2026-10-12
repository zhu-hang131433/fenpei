// Package server 文件分发服务器（源端）Go 实现。
//
// 高并发设计要点：
//   - net/http 每连接一个 goroutine，天然并发；
//   - 文件传输走 http.ServeContent（零拷贝 sendfile + 原生 Range 断点续传），
//     不持有任何状态锁，大量客户端同时拉文件互不阻塞；
//   - 状态用 RWMutex 细粒度保护，落盘改为「合并写」后台协程，
//     高频领任务/心跳不再同步写磁盘。
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"fenpei/internal/discovery"
	"fenpei/internal/protocol"
	"fenpei/internal/update"
	"fenpei/internal/version"
	"fenpei/internal/workspace"
)

const (
	claimTimeoutSec = 300 // worker 超过 5 分钟无心跳则释放其认领的文件夹
	stateFileName   = "distribute_state.json"
	logDirName      = "logs"
)

// ---- 状态结构（与 Python 版 distribute_state.json 兼容）----

type Worker struct {
	Label            string   `json:"label"`
	IP               string   `json:"ip"`
	RegisteredAt     string   `json:"registered_at"`
	LastSeen         string   `json:"last_seen"`
	CompletedFolders []string `json:"completed_folders"`
	FailedFolders    []string `json:"failed_folders"`
	CurrentFolder    string   `json:"current_folder"`

	// 仅运行期使用，不落盘
	Paused        bool   `json:"-"`
	ActiveFolder  string `json:"-"`
	ProgressBytes int64  `json:"-"`
	ProgressFile  string `json:"-"`
}

type Folder struct {
	ClaimToken    string              `json:"claim_token,omitempty"`
	Name          string              `json:"name,omitempty"`
	SourceID      string              `json:"source_id,omitempty"`
	Files         []protocol.FileInfo `json:"files"`
	FileCount     int                 `json:"file_count"`
	TotalSize     int64               `json:"total_size"`
	Status        string              `json:"status"`
	ClaimedBy     *string             `json:"claimed_by"`
	ClaimedAt     *string             `json:"claimed_at"`
	CompletedBy   *string             `json:"completed_by"`
	CompletedAt   *string             `json:"completed_at"`
	Skip          bool                `json:"skip,omitempty"`
	UploadStatus  string              `json:"upload_status,omitempty"`
	UploadMessage string              `json:"upload_message,omitempty"`
	Phase         string              `json:"phase,omitempty"`
	DownloadPct   int                 `json:"download_pct,omitempty"`
	CompressPct   int                 `json:"compress_pct,omitempty"`
	UploadPct     int                 `json:"upload_pct,omitempty"`
	RetryCount    int                 `json:"retry_count,omitempty"`
}

type UploadEntry struct {
	Folder   string `json:"folder"`
	Status   string `json:"status"`
	Message  string `json:"message"`
	WorkerID string `json:"worker_id"`
	Time     string `json:"time"`
}

type State struct {
	PoolVersion  int                     `json:"pool_version,omitempty"`
	Sources      map[string]*Source      `json:"sources,omitempty"`
	SourceDir    string                  `json:"source_dir"`
	StartedAt    string                  `json:"started_at"`
	Paused       bool                    `json:"paused"`
	Workers      map[string]*Worker      `json:"workers"`
	Folders      map[string]*Folder      `json:"folders"`
	UploadStatus map[string]*UploadEntry `json:"upload_status,omitempty"`
}

// Server 文件分发服务端。
type Server struct {
	mu               sync.RWMutex
	scanMu           sync.Mutex
	queueMu          sync.Mutex
	waiters          []*taskWaiter
	queueWake        chan struct{}
	queueDone        chan struct{}
	logMu            sync.Mutex
	evMu             sync.Mutex
	events           []Event
	st               *State
	stateFile        string
	logDir           string
	sourceDir        string
	persistenceError string
	updates          *update.Checker

	dirty     chan struct{}
	stopping  chan struct{}
	saverDone chan struct{}
}

// Event 实时动态（运行期环形缓冲，不落盘）
type Event struct {
	Time    string `json:"time"`
	Type    string `json:"type"`
	Message string `json:"message"`
}

// pushEvent 记录一条动态（线程安全，可与业务锁并存）
func (s *Server) pushEvent(typ, msg string) {
	s.evMu.Lock()
	defer s.evMu.Unlock()
	s.events = append(s.events, Event{Time: protocol.Now(), Type: typ, Message: msg})
	if len(s.events) > 200 {
		s.events = s.events[len(s.events)-200:]
	}
}

// recentEvents 返回最近 n 条（新→旧）
func (s *Server) recentEvents(n int) []Event {
	s.evMu.Lock()
	defer s.evMu.Unlock()
	if len(s.events) > n {
		start := len(s.events) - n
		out := make([]Event, n)
		copy(out, s.events[start:])
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
		return out
	}
	out := make([]Event, len(s.events))
	copy(out, s.events)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func New(sourceDir string) *Server {
	return &Server{
		stateFile: stateFileName,
		updates:   update.NewConfigured(nil),
		logDir:    logDirName,
		sourceDir: sourceDir,
		dirty:     make(chan struct{}, 1),
		stopping:  make(chan struct{}),
		saverDone: make(chan struct{}),
		queueWake: make(chan struct{}, 1),
		queueDone: make(chan struct{}),
		st: &State{
			SourceDir:    "",
			Sources:      map[string]*Source{},
			Workers:      map[string]*Worker{},
			Folders:      map[string]*Folder{},
			UploadStatus: map[string]*UploadEntry{},
		},
	}
}

// Start 加载或扫描状态，并启动后台落盘协程。
func (s *Server) Start() {
	s.loadOrScan()
	go s.saver()
	go s.queueLoop()
}

// Stop 停止后台协程并最终落盘。
func (s *Server) Stop() {
	close(s.stopping)
	<-s.saverDone
	<-s.queueDone
}

func (s *Server) loadOrScan() {
	s.st.StartedAt = protocol.Now()
	if data, err := os.ReadFile(s.stateFile); err == nil {
		var st State
		if err := json.Unmarshal(data, &st); err != nil || st.Folders == nil {
			log.Fatalf("状态文件无效，请先备份并修复 %s；拒绝覆盖已有状态", s.stateFile)
		}
		s.st = &st
	}
	if s.st.Workers == nil {
		s.st.Workers = map[string]*Worker{}
	}
	if s.st.UploadStatus == nil {
		s.st.UploadStatus = map[string]*UploadEntry{}
	}
	if s.st.Sources == nil {
		s.st.Sources = map[string]*Source{}
	}
	if s.st.StartedAt == "" {
		s.st.StartedAt = protocol.Now()
	}
	// 兼容旧单目录状态：任务键与文件路径保留，补全来源关系。
	if s.st.PoolVersion == 0 && s.st.SourceDir != "" {
		id := newSourceID()
		root := s.st.SourceDir
		if resolved, err := filepath.EvalSymlinks(root); err == nil {
			root = resolved
		}
		src := &Source{ID: id, Path: root, Label: filepath.Base(root), AddedAt: protocol.Now()}
		src.Device = deviceOf(root)
		s.st.Sources[id] = src
		for name, f := range s.st.Folders {
			f.SourceID = id
			f.Name = name
		}
	}
	for id := range s.st.Sources {
		if !s.sourceHasFoldersLocked(id) {
			delete(s.st.Sources, id)
		}
	}
	s.st.PoolVersion = 1
	s.st.SourceDir = ""
	if s.sourceDir != "" {
		if _, err := s.addSource(s.sourceDir, ""); err != nil {
			log.Fatalf("添加启动目录失败: %v", err)
		}
	}
	s.saveLocked()
	log.Printf("任务池已恢复：%d 个来源，%d 个文件夹", len(s.st.Sources), len(s.st.Folders))
}

// ---- 落盘（合并写）----

func (s *Server) markDirty() {
	s.notifyQueue()
	select {
	case s.dirty <- struct{}{}:
	default:
	}
}

func (s *Server) saver() {
	defer close(s.saverDone)
	retry := time.NewTicker(5 * time.Second)
	defer retry.Stop()
	for {
		select {
		case <-retry.C:
			s.mu.RLock()
			failed := s.persistenceError != ""
			s.mu.RUnlock()
			if failed {
				s.persist()
			}
		case <-s.stopping:
			s.persist()
			return
		case <-s.dirty:
			time.Sleep(300 * time.Millisecond) // 合并窗口内的多次变更只写一次
			s.persist()
		}
	}
}

func (s *Server) persist() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveLocked()
}

// saveLocked 调用方必须持有写锁。失败保留旧文件，并由后台重试。
func (s *Server) saveLocked() {
	data, err := json.MarshalIndent(s.st, "", "  ")
	if err == nil {
		err = writeStateFile(s.stateFile, data)
	}
	previous := s.persistenceError
	s.persistenceError = ""
	if err != nil {
		s.persistenceError = "任务池状态保存失败：" + err.Error()
		log.Print(s.persistenceError)
		if previous != s.persistenceError {
			s.pushEvent("error", s.persistenceError)
		}
	} else if previous != "" {
		s.pushEvent("state", "任务池状态保存已恢复")
	}
}

func writeStateFile(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".fenpei-state-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0644); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp, path)
}

// ---- 核心业务（锁内）----

func (s *Server) register(ip, clientID string) (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := protocol.Now()
	if clientID != "" {
		if w, ok := s.st.Workers[clientID]; ok {
			w.IP = ip
			w.LastSeen = now
			return clientID, w.Label
		}
	}
	if clientID == "" {
		for wid, w := range s.st.Workers {
			if w.IP == ip {
				w.LastSeen = now
				return wid, w.Label
			}
		}
	}
	n := len(s.st.Workers)
	label := fmt.Sprintf("PC-%03d", n+1)
	wid := clientID
	if wid == "" {
		wid = fmt.Sprintf("w%d", n+1)
	}
	s.st.Workers[wid] = &Worker{
		Label:            label,
		IP:               ip,
		RegisteredAt:     now,
		LastSeen:         now,
		CompletedFolders: []string{},
		FailedFolders:    []string{},
	}
	s.markDirty()
	s.pushEvent("worker", fmt.Sprintf("客户端 %s(%s) 接入", label, ip))
	return wid, label
}

func (s *Server) next(workerID string) (string, *Folder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Paused {
		return "", nil
	}
	w := s.st.Workers[workerID]
	if w == nil || w.Paused {
		return "", nil
	}
	now := time.Now()
	// 释放该 worker 之前 claim 但未完成的文件夹
	for _, f := range s.st.Folders {
		if f.ClaimedBy != nil && *f.ClaimedBy == workerID && f.Status != protocol.StatusCompleted {
			f.Status = protocol.StatusPending
			f.ClaimedBy = nil
			f.ClaimedAt = nil
		}
	}
	// 释放超时的 in_progress 文件夹（worker 超过 5 分钟无心跳）
	for _, f := range s.st.Folders {
		if f.Status == protocol.StatusInProgress && f.ClaimedBy != nil {
			ow := s.st.Workers[*f.ClaimedBy]
			if ow != nil {
				if last, err := time.ParseInLocation(protocol.TimeFormat, ow.LastSeen, time.Local); err == nil {
					if now.Sub(last) > claimTimeoutSec*time.Second {
						f.Status = protocol.StatusPending
						f.ClaimedBy = nil
						f.ClaimedAt = nil
						ow.CurrentFolder = ""
					}
				}
			}
		}
	}
	// 按名字稳定排序，分配第一个 pending 且未跳过的文件夹
	names := make([]string, 0, len(s.st.Folders))
	for n := range s.st.Folders {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		f := s.st.Folders[name]
		if f.Status == protocol.StatusPending && !f.Skip && s.folderAvailable(f) {
			f.Status = protocol.StatusInProgress
			f.ClaimToken = newSourceID()
			cb := workerID
			f.ClaimedBy = &cb
			ca := protocol.Now()
			f.ClaimedAt = &ca
			w.CurrentFolder = name
			w.ActiveFolder = name
			w.LastSeen = protocol.Now()
			s.markDirty()
			s.pushEvent("claim", fmt.Sprintf("%s 领取任务 %s", w.Label, name))
			return name, f
		}
	}
	return "", nil
}

func (s *Server) complete(workerID, folder, claimToken string) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.st.Folders[folder]
	if f == nil {
		return false, "folder not found"
	}
	if s.st.Workers[workerID] == nil || f.Skip || f.ClaimedBy == nil || *f.ClaimedBy != workerID || f.ClaimToken == "" || f.ClaimToken != claimToken {
		return false, "任务领取已失效，请重新领取"
	}
	if f.Status == protocol.StatusCompleted {
		if f.CompletedBy != nil && *f.CompletedBy == workerID {
			return true, "ok"
		}
		return false, "任务已由其他客户端完成"
	}
	if f.Status != protocol.StatusInProgress {
		return false, "任务未处于领取状态"
	}
	f.Status = protocol.StatusCompleted
	cb := workerID
	f.CompletedBy = &cb
	ca := protocol.Now()
	f.CompletedAt = &ca
	if w := s.st.Workers[workerID]; w != nil {
		w.CompletedFolders = append(w.CompletedFolders, folder)
		w.CurrentFolder = ""
		w.LastSeen = protocol.Now()
		s.pushEvent("complete", fmt.Sprintf("%s 完成 %s", w.Label, folder))
	}
	s.markDirty()
	return true, "ok"
}

func (s *Server) setSkip(folder string, skip bool) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.st.Folders[folder]
	if f == nil {
		return false, "folder not found"
	}
	if skip {
		f.Skip = true
		if f.Status == protocol.StatusInProgress {
			if f.ClaimedBy != nil {
				if w := s.st.Workers[*f.ClaimedBy]; w != nil {
					w.CurrentFolder = ""
				}
			}
			f.ClaimedBy = nil
			f.ClaimedAt = nil
		}
		f.Status = protocol.StatusPending
	} else {
		f.Skip = false
	}
	s.markDirty()
	if skip {
		s.pushEvent("skip", "跳过 "+folder)
	} else {
		s.pushEvent("skip", "恢复 "+folder)
	}
	return true, ""
}

func (s *Server) setBatchSkip(folders []string, skip bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, name := range folders {
		f := s.st.Folders[name]
		if f == nil {
			continue
		}
		n++
		if skip {
			f.Skip = true
			if f.Status == protocol.StatusInProgress {
				if f.ClaimedBy != nil {
					if w := s.st.Workers[*f.ClaimedBy]; w != nil {
						w.CurrentFolder = ""
					}
				}
				f.ClaimedBy = nil
				f.ClaimedAt = nil
			}
			f.Status = protocol.StatusPending
		} else {
			f.Skip = false
		}
	}
	s.markDirty()
	action := "批量跳过"
	if !skip {
		action = "批量恢复"
	}
	s.pushEvent("skip", fmt.Sprintf("%s %d 个文件夹", action, n))
	return n
}

func (s *Server) markUploaded(folder string) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.st.Folders[folder]
	if f == nil {
		return false, "folder not found"
	}
	f.Status = protocol.StatusCompleted
	cb := "manual"
	f.CompletedBy = &cb
	ca := protocol.Now()
	f.CompletedAt = &ca
	f.Skip = true
	f.UploadMessage = "手动标记已完成"
	s.markDirty()
	s.pushEvent("manual", folder+" 手动标记已完成")
	return true, ""
}

func (s *Server) setPaused(paused bool) {
	s.mu.Lock()
	s.st.Paused = paused
	s.markDirty()
	s.mu.Unlock()
	if paused {
		log.Println("[管理] 暂停分发")
		s.pushEvent("pause", "暂停分发：不再分配新文件夹")
	} else {
		log.Println("[管理] 恢复分发")
		s.pushEvent("pause", "恢复分发")
	}
}

func (s *Server) setUploadStatus(workerID, folder, status, message string) {
	now := protocol.Now()
	s.mu.Lock()
	if s.st.UploadStatus == nil {
		s.st.UploadStatus = map[string]*UploadEntry{}
	}
	key := workerID + "/" + folder
	s.st.UploadStatus[key] = &UploadEntry{
		Folder:   folder,
		Status:   status,
		Message:  message,
		WorkerID: workerID,
		Time:     now,
	}
	label, ip := workerID, ""
	if f := s.st.Folders[folder]; f != nil {
		f.UploadStatus = status
		f.UploadMessage = message
		switch status {
		case "retrying":
			f.Phase = protocol.PhaseRetrying
		case "retry_exhausted":
			f.Phase = "retry_exhausted"
		case "success", "failed", "error":
			f.Phase = ""
		}
	}
	if w := s.st.Workers[workerID]; w != nil {
		label, ip = w.Label, w.IP
	}
	s.markDirty()
	s.mu.Unlock()

	if status == "success" {
		s.appendUploadLog(fmt.Sprintf("[%s] ✓ 上传成功 | 文件夹: %s | 客户端: %s(%s) | 详情: %s\n",
			now, folder, label, ip, message))
	}
	s.pushEvent("upload", fmt.Sprintf("%s 上传状态: %s", folder, status))
}

func (s *Server) appendUploadLog(line string) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	if err := os.MkdirAll(s.logDir, 0755); err == nil {
		path := filepath.Join(s.logDir, "upload_"+time.Now().Format("20060102")+".log")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			defer f.Close()
			f.WriteString(line)
		}
	}
}

// ---- HTTP 处理 ----

func (s *Server) Handler() http.Handler { return s.ApplicationHandler(nil, nil) }

// ApplicationHandler 复用角色登录与页面；本机统一应用可在登录后启动对应服务。
func (s *Server) ApplicationHandler(onLogin func(string) error, localAPI http.Handler) http.Handler {
	mux := http.NewServeMux()
	checker := s.updates
	checker.Status(false)
	mux.HandleFunc("GET /api/update", checker.Handler)
	mux.Handle("GET /{$}", s.serveIndex())
	mux.Handle("GET /status", s.serveIndex())
	mux.Handle("GET /server", s.serveIndex())
	mux.Handle("GET /server/{$}", s.serveIndex())
	mux.Handle("GET /assets/workspace/", workspace.Assets())
	mux.Handle("GET /assets/", webAssets())
	mux.HandleFunc("GET /api/discovery", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"service": discovery.Service})
	})
	mux.HandleFunc("POST /api/worker/pause", s.handleWorkerPause)
	mux.HandleFunc("GET /api/status", s.handleAPIStatus)
	mux.HandleFunc("GET /api/directories", s.handleDirectories)
	mux.HandleFunc("POST /api/sources/scan", s.handleSourceScan)
	mux.HandleFunc("POST /api/sources", s.handleSourceAdd)
	mux.HandleFunc("POST /api/folders/remove", s.handleRemoveFolder)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("GET /api/events/list", s.handleAPIEvents)
	mux.HandleFunc("GET /api/register", s.handleRegister)
	mux.HandleFunc("GET /api/next", s.handleNext)
	mux.HandleFunc("POST /api/complete", s.handleComplete)
	mux.HandleFunc("POST /api/progress", s.handleProgress)
	mux.HandleFunc("POST /api/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("POST /api/upload_status", s.handleUploadStatus)
	mux.HandleFunc("POST /api/skip", s.handleSkip)
	mux.HandleFunc("POST /api/mark_uploaded", s.handleMarkUploaded)
	mux.HandleFunc("POST /api/pause", s.handlePause)
	mux.HandleFunc("POST /api/batch_skip", s.handleBatchSkip)
	mux.HandleFunc("GET /api/folder_upload_status", s.handleFolderUploadStatus)
	mux.HandleFunc("POST /api/folder_upload_status", s.handleFolderUploadStatus)
	mux.HandleFunc("POST /api/folder_progress", s.handleFolderProgress)
	mux.HandleFunc("GET /files/", s.handleFile)
	mux.HandleFunc("GET /client", s.handleClientBinary)
	mux.HandleFunc("GET /client.py", s.handleLegacyScript("client.py"))
	mux.HandleFunc("GET /auto_upload.py", s.handleLegacyScript("auto_upload.py"))
	auth := newWebAuth()
	auth.onLogin = onLogin
	auth.localWorkspace = localAPI != nil
	if localAPI != nil {
		mux.Handle("/api/local/", localAPI)
	}
	return auth.wrap(mux)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, out any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	return dec.Decode(out) == nil
}

func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	wid, label := s.register(remoteIP(r), r.Header.Get("X-Client-Id"))
	writeJSON(w, 200, protocol.RegisterResp{WorkerID: wid, Label: label, IP: remoteIP(r)})
}

func (s *Server) handleNext(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Pool-Protocol") != "2" {
		writeJSON(w, 426, map[string]string{"error": "任务池需要新版客户端，请从 /client 重新下载并替换客户端"})
		return
	}
	workerID := r.Header.Get("X-Worker-Id")
	s.mu.RLock()
	known := workerID != "" && s.st.Workers[workerID] != nil
	s.mu.RUnlock()
	if !known {
		writeJSON(w, 401, map[string]string{"error": "unregistered worker"})
		return
	}
	s.awaitTask(w, r, workerID)
}

func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	var req protocol.CompleteReq
	if !readJSON(r, &req) || req.Folder == "" {
		writeJSON(w, 400, map[string]string{"error": "missing folder"})
		return
	}
	ok, msg := s.complete(r.Header.Get("X-Worker-Id"), req.Folder, req.ClaimToken)
	writeJSON(w, 200, map[string]any{"success": ok, "message": msg})
}

func (s *Server) handleProgress(w http.ResponseWriter, r *http.Request) {
	var req map[string]any
	readJSON(r, &req)
	workerID := r.Header.Get("X-Worker-Id")
	s.mu.Lock()
	if w := s.st.Workers[workerID]; w != nil {
		w.LastSeen = protocol.Now()
		if b, ok := req["bytes"].(float64); ok {
			w.ProgressBytes = int64(b)
		}
		if f, ok := req["file"].(string); ok {
			w.ProgressFile = f
		}
	}
	s.mu.Unlock()
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	workerID := r.Header.Get("X-Worker-Id")
	s.mu.Lock()
	if w := s.st.Workers[workerID]; w != nil {
		w.LastSeen = protocol.Now()
	}
	s.mu.Unlock()
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func (s *Server) handleUploadStatus(w http.ResponseWriter, r *http.Request) {
	var req protocol.UploadStatusReq
	if !readJSON(r, &req) {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	s.setUploadStatus(r.Header.Get("X-Worker-Id"), req.Folder, req.Status, req.Message)
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func (s *Server) handleSkip(w http.ResponseWriter, r *http.Request) {
	var req protocol.SkipReq
	if !readJSON(r, &req) {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	ok, msg := s.setSkip(req.Folder, req.Skip)
	if !ok {
		writeJSON(w, 404, map[string]string{"error": msg})
		return
	}
	action := "跳过"
	if !req.Skip {
		action = "恢复"
	}
	log.Printf("[管理] %s文件夹: %s", action, req.Folder)
	writeJSON(w, 200, map[string]any{"ok": true, "folder": req.Folder, "skip": req.Skip})
}

func (s *Server) handleMarkUploaded(w http.ResponseWriter, r *http.Request) {
	var req protocol.CompleteReq
	if !readJSON(r, &req) {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	ok, msg := s.markUploaded(req.Folder)
	if !ok {
		writeJSON(w, 404, map[string]string{"error": msg})
		return
	}
	log.Printf("[管理] 手动标记已完成: %s", req.Folder)
	writeJSON(w, 200, map[string]any{"ok": true, "folder": req.Folder})
}

func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	var req protocol.PauseReq
	readJSON(r, &req)
	s.setPaused(req.Paused)
	writeJSON(w, 200, map[string]any{"ok": true, "paused": req.Paused})
}

func (s *Server) handleBatchSkip(w http.ResponseWriter, r *http.Request) {
	var req protocol.BatchSkipReq
	if !readJSON(r, &req) {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	n := s.setBatchSkip(req.Folders, req.Skip)
	action := "跳过"
	if !req.Skip {
		action = "恢复"
	}
	log.Printf("[管理] 批量%s %d 个文件夹", action, n)
	writeJSON(w, 200, map[string]any{"ok": true, "count": n})
}

func (s *Server) handleFolderUploadStatus(w http.ResponseWriter, r *http.Request) {
	folder := ""
	if q := r.URL.Query().Get("folder"); q != "" {
		folder = q
	} else {
		var req protocol.CompleteReq
		readJSON(r, &req)
		folder = req.Folder
	}
	s.mu.RLock()
	f := s.st.Folders[folder]
	resp := protocol.FolderUploadStatusResp{}
	if f != nil {
		resp.UploadStatus = f.UploadStatus
		resp.Status = f.Status
		resp.Skip = f.Skip
	}
	s.mu.RUnlock()
	writeJSON(w, 200, resp)
}

func (s *Server) handleFolderProgress(w http.ResponseWriter, r *http.Request) {
	var req protocol.FolderProgressReq
	if !readJSON(r, &req) {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	workerID := r.Header.Get("X-Worker-Id")
	s.mu.Lock()
	if w := s.st.Workers[workerID]; w != nil {
		w.LastSeen = protocol.Now()
		if s.st.Folders[req.Folder] != nil {
			w.ActiveFolder = req.Folder
		}
	}
	if f := s.st.Folders[req.Folder]; f != nil {
		if req.Phase != "" {
			f.Phase = req.Phase
		}
		f.DownloadPct = req.DownloadPct
		f.CompressPct = req.CompressPct
		f.UploadPct = req.UploadPct
		f.RetryCount = req.RetryCount
	}
	s.mu.Unlock()
	writeJSON(w, 200, map[string]bool{"ok": true})
}

// handleFile 只读发送文件，http.ServeContent 原生支持 Range 断点续传与 sendfile 零拷贝。
func (s *Server) handleFile(w http.ResponseWriter, r *http.Request) {
	full, err := s.resolvePoolFile(strings.TrimPrefix(r.URL.Path, "/files/"))
	if err != nil {
		writeJSON(w, 404, map[string]string{"error": err.Error()})
		return
	}
	f, err := os.Open(full)
	if err != nil {
		writeJSON(w, 404, map[string]string{"error": "数据来源不可读取"})
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() {
		writeJSON(w, 404, map[string]string{"error": "文件不存在"})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	http.ServeContent(w, r, fi.Name(), fi.ModTime(), f)
}

func (s *Server) exeDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(exe)
}

func (s *Server) handleClientBinary(w http.ResponseWriter, r *http.Request) {
	p := filepath.Join(s.exeDir(), "fenpei-client")
	if _, err := os.Stat(p); err != nil {
		writeJSON(w, 404, map[string]string{"error": "fenpei-client not found next to server binary"})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeFile(w, r, p)
}

func (s *Server) handleLegacyScript(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p := filepath.Join(s.exeDir(), name)
		if _, err := os.Stat(p); err != nil {
			writeJSON(w, 404, map[string]string{"error": name + " not found"})
			return
		}
		w.Header().Set("Content-Type", "text/x-python")
		w.Header().Set("Content-Disposition", "attachment; filename="+name)
		http.ServeFile(w, r, p)
	}
}

type apiFolderInfo struct {
	CanDelete        bool    `json:"can_delete"`
	Name             string  `json:"name"`
	SourceID         string  `json:"source_id"`
	SourceLabel      string  `json:"source_label"`
	SourcePath       string  `json:"source_path"`
	Available        bool    `json:"available"`
	Status           string  `json:"status"`
	Skip             bool    `json:"skip"`
	FileCount        int     `json:"file_count"`
	TotalSize        int64   `json:"total_size"`
	ClaimedBy        *string `json:"claimed_by"`
	ClaimedByLabel   string  `json:"claimed_by_label,omitempty"`
	CompletedBy      *string `json:"completed_by"`
	CompletedByLabel string  `json:"completed_by_label,omitempty"`
	UploadStatus     string  `json:"upload_status,omitempty"`
	UploadMessage    string  `json:"upload_message,omitempty"`
	Phase            string  `json:"phase,omitempty"`
	DownloadPct      int     `json:"download_pct,omitempty"`
	CompressPct      int     `json:"compress_pct,omitempty"`
	UploadPct        int     `json:"upload_pct,omitempty"`
	RetryCount       int     `json:"retry_count,omitempty"`
}

type apiWorkerInfo struct {
	Paused         bool   `json:"paused"`
	Waiting        bool   `json:"waiting"`
	Label          string `json:"label"`
	IP             string `json:"ip"`
	CurrentFolder  string `json:"current_folder"`
	ActiveFolder   string `json:"active_folder"`
	CompletedCount int    `json:"completed_count"`
	LastSeen       string `json:"last_seen"`
	Online         bool   `json:"online"`
}

type statusSnapshot struct {
	PersistenceError string                    `json:"persistence_error,omitempty"`
	Sources          []SourceView              `json:"sources"`
	SourceDir        string                    `json:"source_dir"`
	StartedAt        string                    `json:"started_at"`
	Paused           bool                      `json:"paused"`
	Version          string                    `json:"version"`
	TotalFolders     int                       `json:"total_folders"`
	Completed        int                       `json:"completed"`
	Uploaded         int                       `json:"uploaded"`
	InProgress       int                       `json:"in_progress"`
	Pending          int                       `json:"pending"`
	Workers          map[string]*apiWorkerInfo `json:"workers"`
	Folders          map[string]*apiFolderInfo `json:"folders_summary"`
	Events           []Event                   `json:"events"`
}

// snapshot 生成全量状态快照（/api/status 与 SSE 共用）。
func (s *Server) snapshot() *statusSnapshot {
	waiting := s.waitingWorkers()
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	total := len(s.st.Folders)
	done, inprog, uploaded := 0, 0, 0
	summary := make(map[string]*apiFolderInfo, total)
	sourceViews, availability := s.sourceViewsLocked()
	for name, f := range s.st.Folders {
		switch f.Status {
		case protocol.StatusCompleted:
			done++
		case protocol.StatusInProgress:
			inprog++
		}
		if f.UploadStatus == "success" {
			uploaded++
		}
		fi := &apiFolderInfo{
			CanDelete: !s.folderBusyLocked(name),
			Name:      f.Name, SourceID: f.SourceID, Available: availability[f.SourceID] && s.folderAvailable(f),
			Status:        f.Status,
			Skip:          f.Skip,
			FileCount:     f.FileCount,
			TotalSize:     f.TotalSize,
			ClaimedBy:     f.ClaimedBy,
			CompletedBy:   f.CompletedBy,
			UploadStatus:  f.UploadStatus,
			UploadMessage: f.UploadMessage,
			Phase:         f.Phase,
			DownloadPct:   f.DownloadPct,
			CompressPct:   f.CompressPct,
			UploadPct:     f.UploadPct,
			RetryCount:    f.RetryCount,
		}
		if src := s.st.Sources[f.SourceID]; src != nil {
			fi.SourceLabel = src.Label
			fi.SourcePath = src.Path
		}
		if f.ClaimedBy != nil {
			if wk := s.st.Workers[*f.ClaimedBy]; wk != nil {
				fi.ClaimedByLabel = wk.Label
			}
		}
		if f.CompletedBy != nil {
			if wk := s.st.Workers[*f.CompletedBy]; wk != nil {
				fi.CompletedByLabel = wk.Label
			}
		}
		summary[name] = fi
	}
	workers := make(map[string]*apiWorkerInfo, len(s.st.Workers))
	for wid, w := range s.st.Workers {
		online := false
		if last, err := time.ParseInLocation(protocol.TimeFormat, w.LastSeen, time.Local); err == nil {
			online = now.Sub(last) < 15*time.Second
		}
		workers[wid] = &apiWorkerInfo{
			Paused:         w.Paused,
			Waiting:        waiting[wid],
			Label:          w.Label,
			IP:             w.IP,
			CurrentFolder:  w.CurrentFolder,
			ActiveFolder:   w.ActiveFolder,
			CompletedCount: len(w.CompletedFolders),
			LastSeen:       w.LastSeen,
			Online:         online,
		}
	}
	return &statusSnapshot{
		PersistenceError: s.persistenceError,
		Sources:          sourceViews,
		SourceDir:        s.st.SourceDir,
		StartedAt:        s.st.StartedAt,
		Paused:           s.st.Paused,
		Version:          version.Version,
		TotalFolders:     total,
		Completed:        done,
		Uploaded:         uploaded,
		InProgress:       inprog,
		Pending:          total - done - inprog,
		Workers:          workers,
		Folders:          summary,
		Events:           s.recentEvents(50),
	}
}

func (s *Server) handleAPIStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, s.snapshot())
}

// handleEvents SSE 实时事件流：每 2s 推送一次全量快照，前端断线自动重连。
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, 500, map[string]string{"error": "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(200)

	send := func() {
		b, err := json.Marshal(s.snapshot())
		if err != nil {
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
	}
	send() // 首帧立即推送

	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
			send()
		}
	}
}

// handleAPIEvents 仅拉取动态列表
func (s *Server) handleAPIEvents(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"events": s.recentEvents(200)})
}
