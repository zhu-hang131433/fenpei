package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"fenpei/internal/discovery"
	"fenpei/internal/protocol"
	"fenpei/internal/update"
	"fenpei/internal/uploader"
	"fenpei/internal/workspace"
)

type AgentOptions struct {
	Options
	LocalPort     int
	DiscoveryPort int
	OpenBrowser   bool
}
type LogEntry struct {
	Time    string `json:"time"`
	Message string `json:"message"`
}
type AgentStatus struct {
	Server    string     `json:"server"`
	Connected bool       `json:"connected"`
	Running   bool       `json:"running"`
	Paused    bool       `json:"paused"`
	Upload    bool       `json:"upload"`
	Dest      string     `json:"dest"`
	Error     string     `json:"error"`
	Work      WorkState  `json:"work"`
	Logs      []LogEntry `json:"logs"`
}
type Agent struct {
	mu       sync.Mutex
	control  sync.Mutex
	opt      AgentOptions
	state    AgentStatus
	command  *exec.Cmd
	workerID string
	logFile  *os.File
}

func NewAgent(opt AgentOptions) (*Agent, error) {
	if opt.LocalPort == 0 {
		opt.LocalPort = 18080
	}
	if opt.DiscoveryPort == 0 {
		opt.DiscoveryPort = 8000
	}
	if opt.UploadRetries <= 0 {
		opt.UploadRetries = uploader.DefaultMaxRetries
	}
	if opt.Threads <= 0 {
		opt.Threads = 4
	}
	if opt.Files <= 0 {
		opt.Files = 2
	}
	if opt.Chunk <= 0 {
		opt.Chunk = 4 << 20
	}
	if opt.Dest == "" {
		opt.Dest = "./received"
	}
	dest, err := filepath.Abs(opt.Dest)
	if err != nil {
		return nil, err
	}
	opt.Dest = dest
	if err := os.MkdirAll(filepath.Join(dest, "logs"), 0755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(dest, "logs", "client_"+time.Now().Format("20060102")+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	a := &Agent{opt: opt, workerID: loadOrCreateID(filepath.Join(dest, ".client_id")), logFile: file}
	a.state = AgentStatus{Dest: dest, Upload: opt.Upload, Logs: []LogEntry{}}
	a.addLog("客户端已启动，正在自动寻找同一局域网内的服务端。")
	return a, nil
}
func (a *Agent) addLog(message string) {
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	if len(message) > 4000 {
		message = message[:4000]
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	entry := LogEntry{Time: protocol.Now(), Message: message}
	a.state.Logs = append(a.state.Logs, entry)
	if len(a.state.Logs) > 500 {
		a.state.Logs = append([]LogEntry{}, a.state.Logs[len(a.state.Logs)-500:]...)
	}
	fmt.Fprintf(a.logFile, "%s %s\n", entry.Time, entry.Message)
}
func (a *Agent) Snapshot() AgentStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.state
	s.Logs = append([]LogEntry{}, a.state.Logs...)
	return s
}
func (a *Agent) Connect(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		a.mu.Lock()
		base := a.state.Server
		a.mu.Unlock()
		if base == "" {
			var err error
			if a.opt.Server != "" {
				base = strings.TrimRight(a.opt.Server, "/")
				err = discovery.Probe(ctx, base)
			} else {
				base, err = discovery.Find(ctx, a.opt.DiscoveryPort, 1200*time.Millisecond)
			}
			if err != nil {
				a.mu.Lock()
				a.state.Connected = false
				a.state.Error = err.Error()
				a.mu.Unlock()
				if !waitContext(ctx, 3*time.Second) {
					return
				}
				continue
			}
		}
		var reg protocol.RegisterResp
		err := protocol.JSONRequest("GET", base+"/api/register", nil, map[string]string{"X-Client-Id": a.workerID}, 2*time.Second, &reg)
		a.mu.Lock()
		wasConnected := a.state.Connected
		if err == nil {
			a.state.Server = base
			a.state.Connected = true
			a.state.Error = ""
			a.state.Work.WorkerID = reg.WorkerID
			a.state.Work.Label = reg.Label
		} else {
			a.state.Connected = false
			a.state.Error = "服务端连接中断，正在自动重连"
		}
		a.mu.Unlock()
		if err == nil && !wasConnected {
			a.addLog("已自动连接服务端 " + base + "，本机编号 " + reg.Label + "。点击开始工作后才领取任务。")
		}
		if err != nil && wasConnected {
			a.addLog("服务端连接中断，自动重连期间保留当前任务。")
		}
		a.control.Lock()
		state := a.Snapshot()
		if err == nil && state.Paused {
			a.sendPaused(true)
			if state.Work.Progress.Folder != "" {
				protocol.JSONRequest("POST", base+"/api/folder_progress", state.Work.Progress, map[string]string{"X-Worker-Id": a.workerID}, 2*time.Second, nil)
			}
		}
		a.control.Unlock()
		if !waitContext(ctx, 3*time.Second) {
			return
		}
	}
}
func waitContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (a *Agent) sendPaused(paused bool) error {
	a.mu.Lock()
	base := a.state.Server
	a.mu.Unlock()
	if base == "" {
		return nil
	}
	return protocol.JSONRequest("POST", base+"/api/worker/pause", protocol.PauseReq{Paused: paused}, map[string]string{"X-Worker-Id": a.workerID}, 2*time.Second, nil)
}
func (a *Agent) Start() error {
	a.control.Lock()
	defer a.control.Unlock()
	state := a.Snapshot()
	if !state.Connected {
		return fmt.Errorf("正在寻找或重连服务端，请稍后")
	}
	a.mu.Lock()
	cmd := a.command
	a.mu.Unlock()
	if cmd != nil {
		if !state.Paused {
			return nil
		}
		// 先解除队列暂停，再恢复同一进程的下载、压缩、上传与缓存。
		if err := a.sendPaused(false); err != nil {
			return fmt.Errorf("服务端暂不可连接: %w", err)
		}
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGCONT); err != nil {
			return err
		}
		a.mu.Lock()
		a.state.Paused = false
		a.mu.Unlock()
		a.addLog("继续工作：恢复当前任务。")
		return nil
	}
	if err := a.sendPaused(false); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	args := []string{"--worker", "--server", state.Server, "--dest", a.opt.Dest, "--threads", fmt.Sprint(a.opt.Threads), "--files", fmt.Sprint(a.opt.Files), "--chunk", fmt.Sprint(a.opt.Chunk), "--upload-retries", fmt.Sprint(a.opt.UploadRetries), "--upload=" + fmt.Sprint(a.opt.Upload)}
	cmd = exec.Command(exe, args...)
	// 工作进程独立进程组；控制进程退出时不留下被暂停的孤儿进程。
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	output, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout
	finished := make(chan struct{})
	if err = startWorkerProcess(cmd, finished); err != nil {
		output.Close()
		return err
	}
	a.mu.Lock()
	a.command = cmd
	a.state.Running = true
	a.state.Paused = false
	a.state.Work.Progress = protocol.FolderProgressReq{}
	a.state.Work.Completed = 0
	a.state.Work.Failed = 0
	a.mu.Unlock()
	a.addLog("开始工作：领取任务，自动下载" + map[bool]string{true: "、压缩并上传。", false: "。"}[a.opt.Upload])
	go a.readOutput(cmd, output, finished)
	return nil
}
func (a *Agent) Pause() error {
	a.control.Lock()
	defer a.control.Unlock()
	a.mu.Lock()
	cmd := a.command
	paused := a.state.Paused
	a.mu.Unlock()
	if cmd == nil || paused {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGSTOP); err != nil {
		return err
	}
	a.mu.Lock()
	a.state.Paused = true
	a.mu.Unlock()
	a.addLog("已立即暂停下载、压缩和上传；独立心跳继续保持任务归属。")
	if err := a.sendPaused(true); err != nil {
		a.addLog("本机工作已暂停；服务端暂未确认暂停状态，将自动重试。")
	}
	return nil
}
func (a *Agent) readOutput(cmd *exec.Cmd, output io.ReadCloser, finished chan struct{}) {
	defer close(finished)
	scanner := bufio.NewScanner(output)
	scanner.Buffer(make([]byte, 4096), 128<<10)
	// 终端进度以回车刷新，不把进度动画反复写入日志。
	for scanner.Scan() {
		text := scanner.Text()
		if index := strings.Index(text, StateMarker); index >= 0 {
			var work WorkState
			if json.Unmarshal([]byte(text[index+len(StateMarker):]), &work) == nil {
				a.mu.Lock()
				a.state.Work = work
				a.mu.Unlock()
			}
			continue
		}
		if strings.Contains(text, "\r") {
			parts := strings.Split(text, "\r")
			text = parts[len(parts)-1]
			if strings.Contains(text, "[%") || strings.Contains(text, "总速") {
				continue
			}
		}
		a.addLog(text)
	}
	if err := scanner.Err(); err != nil {
		a.addLog("读取工作日志失败: " + err.Error())
	}
	err := cmd.Wait()
	output.Close()
	a.control.Lock()
	a.mu.Lock()
	if a.command == cmd {
		a.command = nil
		a.state.Running = false
		a.state.Paused = false
	}
	a.mu.Unlock()
	a.control.Unlock()
	if err != nil {
		a.addLog("工作进程结束: " + err.Error())
	} else {
		a.addLog("本次工作结束。")
	}
}
func (a *Agent) Close() {
	a.control.Lock()
	a.mu.Lock()
	cmd := a.command
	a.mu.Unlock()
	if cmd != nil {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	a.control.Unlock()
	// 工作日志 goroutine 可能仍在收尾，保留文件到进程退出。
}
func (a *Agent) Handler(port int) http.Handler {
	mux := http.NewServeMux()
	checker := update.NewConfigured(func() string { return a.Snapshot().Server })
	checker.Status(false)
	mux.HandleFunc("GET /api/local/update", checker.Handler)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { workspace.Page(w, r, true) })
	mux.Handle("GET /assets/workspace/", workspace.Assets())
	mux.HandleFunc("GET /api/local/status", func(w http.ResponseWriter, r *http.Request) { writeLocalJSON(w, 200, a.Snapshot()) })
	for path, action := range map[string]func() error{"/api/local/start": a.Start, "/api/local/pause": a.Pause} {
		action := action
		mux.HandleFunc("POST "+path, func(w http.ResponseWriter, r *http.Request) {
			if err := action(); err != nil {
				writeLocalJSON(w, 409, map[string]string{"error": err.Error()})
				return
			}
			writeLocalJSON(w, 200, map[string]bool{"ok": true})
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 回环监听加 Host/Origin 校验，防止其他网页控制本机工作进程。
		expectedHost := net.JoinHostPort("127.0.0.1", fmt.Sprint(port))
		localhost := net.JoinHostPort("localhost", fmt.Sprint(port))
		if r.Host != expectedHost && r.Host != localhost {
			http.Error(w, "invalid host", 403)
			return
		}
		origin := r.Header.Get("Origin")
		server := a.Snapshot().Server
		allowed := origin == "" || origin == "http://"+expectedHost || origin == "http://"+localhost || origin == server
		if !allowed {
			http.Error(w, "invalid origin", 403)
			return
		}
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Allow-Private-Network", "true")
		}
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		if r.Method == "POST" && !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			http.Error(w, "expected JSON", 415)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}
func writeLocalJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
func RunAgent(ctx context.Context, opt AgentOptions) error {
	agent, err := NewAgent(opt)
	if err != nil {
		return err
	}
	defer agent.Close()
	address := net.JoinHostPort("127.0.0.1", fmt.Sprint(agent.opt.LocalPort))
	listener, err := net.Listen("tcp4", address)
	if err != nil {
		return fmt.Errorf("客户端页面端口不可用（是否已经启动客户端？）: %w", err)
	}
	server := &http.Server{Handler: agent.Handler(agent.opt.LocalPort), ReadHeaderTimeout: 5 * time.Second}
	go agent.Connect(ctx)
	go func() { <-ctx.Done(); server.Close() }()
	pageURL := "http://" + address
	fmt.Println("客户端页面:", pageURL, "；正在自动发现服务端，点击开始工作后执行任务。")
	if opt.OpenBrowser {
		go func() {
			if err := exec.Command("xdg-open", pageURL).Run(); err != nil {
				fmt.Println("请在浏览器打开:", pageURL)
			}
		}()
	}
	err = server.Serve(listener)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// 保留 URL 导入用于规范配置地址，避免尾部斜线和非 HTTP 地址。
func ValidServer(base string) bool {
	u, err := url.Parse(base)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

// Linux 的 Pdeathsig 跟随创建子进程的线程；固定该线程直至子进程退出，
// 防止 Go 回收空闲线程时误杀仍在下载/上传的工作进程。
func startWorkerProcess(cmd *exec.Cmd, finished <-chan struct{}) error {
	started := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		err := cmd.Start()
		started <- err
		if err == nil {
			<-finished
		}
	}()
	return <-started
}
