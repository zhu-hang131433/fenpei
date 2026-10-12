// Package client 文件分发客户端（目标电脑端）Go 实现。
//
// 高并发设计要点：
//   - 文件级并行：同一文件夹内多个文件同时下载（--files 控制）；
//   - 分块级并行：大文件按 Range 切块，多 goroutine 并行下载（--threads 控制）；
//   - 全局并发受 worker 池约束，避免打满磁盘/网络队列；
//   - 每块独立重试（指数退避），单块失败只重传该块；
//   - 复用 HTTP 连接池（Keep-Alive），断点续传以「块」为粒度。
package client

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"fenpei/internal/protocol"
	"fenpei/internal/uploader"
)

const (
	minParallelSize = 5 << 20 // 5MB 以上走并行分块
	maxRetry        = 5
	chunkReqTimeout = 15 * time.Minute // 单块请求超时（防僵死连接）
)

type Options struct {
	Observe func(WorkState) // 本地页面可选进度回调

	UploadRetries int // 自动上传最大重试次数，不包含首次尝试
	Server        string
	Dest          string
	Threads       int // 每文件分块并行数
	Files         int // 文件级并行数
	Chunk         int64
	Upload        bool
}

type Client struct {
	opt      Options
	workerID string
	label    string

	pmu  sync.Mutex
	prog protocol.FolderProgressReq

	completed   atomic.Int64
	failed      atomic.Int64
	doneBytes   atomic.Int64
	totalBytes  int64
	folderStart time.Time
}

// Run 客户端主循环。
func Run(opt Options) error {
	if opt.Threads <= 0 {
		opt.Threads = 4
	}
	if opt.Files <= 0 {
		opt.Files = 2
	}
	if opt.Chunk <= 0 {
		opt.Chunk = 4 << 20
	}
	c := &Client{opt: opt}
	server := opt.Server
	if err := os.MkdirAll(opt.Dest, 0755); err != nil {
		return fmt.Errorf("创建保存目录失败: %w", err)
	}

	line(60)
	fmt.Println("文件分发客户端 [Go 版]")
	fmt.Println("服务器:", server)
	fmt.Println("保存目录:", opt.Dest)
	fmt.Printf("文件级并行: %d  每文件分块线程: %d  分块: %.1fMB\n", opt.Files, opt.Threads, float64(opt.Chunk)/1024/1024)
	if opt.Upload {
		fmt.Println("自动上传: 已启用（吉翼数采平台）")
	}
	line(60)

	clientID := loadOrCreateID(filepath.Join(opt.Dest, ".client_id"))

	var reg protocol.RegisterResp
	err := protocol.JSONRequest("GET", server+"/api/register", nil,
		map[string]string{"X-Client-Id": clientID}, 30*time.Second, &reg)
	if err != nil {
		return fmt.Errorf("注册失败: %w", err)
	}
	c.workerID, c.label = reg.WorkerID, reg.Label
	fmt.Printf("注册成功！本机标号: %s\n\n", c.label)

	c.emit()
	reportDone := make(chan struct{})
	defer close(reportDone)
	go c.reportLoop(server, reportDone)

	completed := 0
	for {
		var task protocol.NextResp
		err := protocol.JSONRequest("GET", server+"/api/next", nil,
			map[string]string{"X-Worker-Id": c.workerID, "X-Pool-Protocol": "2"}, 60*time.Second, &task)
		if err != nil {
			fmt.Println("领取任务失败:", err)
			time.Sleep(3 * time.Second)
			continue
		}
		if task.Waiting {
			// 服务端保留领取请求；正常长轮询到期后立即重新入队。
			c.setProg("", "", 0, 0, 0, 0)
			continue
		}

		if task.Done {
			fmt.Println("\n所有文件夹已分发完成！")
			break
		}
		if c.processFolder(server, &task) {
			completed++
			c.completed.Add(1)
		} else {
			c.failed.Add(1)
		}
		c.emit()
	}

	line(60)
	fmt.Printf("本次会话完成: %d 个文件夹\n", completed)
	line(60)
	return nil
}

func line(n int) {
	for i := 0; i < n; i++ {
		fmt.Print("=")
	}
	fmt.Println()
}

func loadOrCreateID(path string) string {
	if b, err := os.ReadFile(path); err == nil && len(b) >= 8 {
		return string(b)
	}
	b := make([]byte, 16)
	rand.Read(b)
	id := hex.EncodeToString(b)
	os.WriteFile(path, []byte(id), 0644)
	return id
}

// ---- 进度上报 ----

func (c *Client) setProg(folder, phase string, d, cp, up, rc int) {
	c.pmu.Lock()
	c.prog = protocol.FolderProgressReq{
		Folder:      folder,
		Phase:       phase,
		DownloadPct: d,
		CompressPct: cp,
		UploadPct:   up,
		RetryCount:  rc,
	}
	c.pmu.Unlock()
	c.emit()
}

func (c *Client) updateProg(phase string, compressPct, uploadPct, retryCount int) {
	c.pmu.Lock()
	if phase != "" {
		c.prog.Phase = phase
	}
	if compressPct >= 0 {
		c.prog.CompressPct = compressPct
	}
	if uploadPct >= 0 {
		c.prog.UploadPct = uploadPct
	}
	if retryCount >= 0 {
		c.prog.RetryCount = retryCount
	}
	c.pmu.Unlock()
	c.emit()
}

func (c *Client) reportLoop(server string, done <-chan struct{}) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	lastIdleHeartbeat := time.Time{}
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}
		c.pmu.Lock()
		data := c.prog
		c.pmu.Unlock()
		if data.Folder == "" {
			if time.Since(lastIdleHeartbeat) < 9*time.Second {
				continue
			}
			protocol.JSONRequest("POST", server+"/api/heartbeat", nil, map[string]string{"X-Worker-Id": c.workerID}, 15*time.Second, nil)
			lastIdleHeartbeat = time.Now()
			continue
		}
		protocol.JSONRequest("POST", server+"/api/folder_progress", data,
			map[string]string{"X-Worker-Id": c.workerID}, 15*time.Second, nil)
	}
}

// ---- 文件夹处理 ----

func (c *Client) processFolder(server string, task *protocol.NextResp) bool {
	folder := task.Folder
	needed := task.TotalSize + task.MaxFolderSize
	c.totalBytes = task.TotalSize
	c.doneBytes.Store(0)
	c.setProg(folder, "disk_wait", 0, 0, 0, 0)

	// 磁盘空间检查：保证下载后仍能容纳一个最大文件夹（用于压缩）
	for {
		free := freeBytes(c.opt.Dest)
		if free >= uint64(needed) {
			break
		}
		fmt.Printf("  ⚠ 空间不足！可用 %.1fGB，需要 %.1fGB（下载%.1f + 预留压缩%.1f），30s 后重查...\n",
			gb(int64(free)), gb(needed), gb(task.TotalSize), gb(task.MaxFolderSize))
		c.setProg(folder, "disk_wait", 0, 0, 0, 0)
		time.Sleep(30 * time.Second)
	}

	fmt.Printf("[领取文件夹] %s (%d 文件, %s)\n", folder, task.FileCount,
		protocol.HumanSize(float64(task.TotalSize)))
	fmt.Printf("  磁盘可用: %.1fGB  需要: %.1fGB\n", gb(int64(freeBytes(c.opt.Dest))), gb(needed))

	c.doneBytes.Store(0)
	c.totalBytes = task.TotalSize
	c.folderStart = time.Now()
	c.setProg(folder, protocol.PhaseDownloading, 0, 0, 0, 0)

	stopBar := make(chan struct{})
	var barWG sync.WaitGroup
	barWG.Add(1)
	go func() {
		defer barWG.Done()
		t := time.NewTicker(800 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopBar:
				return
			case <-t.C:
				c.drawBar()
			}
		}
	}()

	if err := c.quarantineExtraFiles(task); err != nil {
		close(stopBar)
		barWG.Wait()
		fmt.Printf("清理旧任务文件失败: %v\n", err)
		return false
	}
	ok := c.downloadAll(task.Files)

	close(stopBar)
	barWG.Wait()
	c.drawBar()
	fmt.Println()

	if ok {
		var resp protocol.SimpleResp
		if err := protocol.JSONRequest("POST", server+"/api/complete", protocol.CompleteReq{Folder: folder, ClaimToken: task.ClaimToken},
			map[string]string{"X-Worker-Id": c.workerID}, 30*time.Second, &resp); err != nil || !resp.Success {
			fmt.Printf("  下载完成但服务端确认失败: %v %s\n", err, resp.Message)
			c.setProg("", "", 0, 0, 0, 0)
			return false
		}
		fmt.Printf("  文件夹完成 ✓: %s\n\n", folder)
		if c.opt.Upload {
			name := task.FolderName
			if name == "" {
				name = filepath.Base(folder)
			}
			ok = c.uploadFolder(server, folder, name, task.ClaimToken)
		}
	} else {
		fmt.Printf("  文件夹内有文件下载失败: %s\n\n", folder)
	}
	c.setProg("", "", 0, 0, 0, 0)
	return ok
}

func (c *Client) drawBar() {
	if c.totalBytes <= 0 {
		return
	}
	done := c.doneBytes.Load()
	pct := float64(done) * 100 / float64(c.totalBytes)
	if pct > 100 {
		pct = 100
	}
	if pct < 0 {
		pct = 0
	}
	c.pmu.Lock()
	c.prog.DownloadPct = int(pct)
	c.pmu.Unlock()
	c.emit()
	elapsed := time.Since(c.folderStart).Seconds()
	speed := float64(done) / elapsed / (1024 * 1024)
	remain := 0.0
	if speed > 0 {
		remain = float64(c.totalBytes-done) / (speed * 1024 * 1024)
	}
	barLen := 30
	filled := int(float64(barLen) * pct / 100)
	if filled > barLen {
		filled = barLen
	}
	bar := ""
	for i := 0; i < barLen; i++ {
		if i < filled {
			bar += "="
		} else {
			bar += " "
		}
	}
	fmt.Printf("\r  [%s] %5.1f%%  %s/%s  总速%.1fMB/s  剩余%.0fs   ",
		bar, pct, protocol.HumanSize(float64(done)), protocol.HumanSize(float64(c.totalBytes)), speed, remain)
}

// 把不属于当前清单的旧文件移到任务目录外，避免压缩时混入旧数据。
func (c *Client) quarantineExtraFiles(task *protocol.NextResp) error {
	root := filepath.Join(c.opt.Dest, task.Folder)
	expected := map[string]bool{}
	for _, f := range task.Files {
		expected[filepath.Clean(filepath.Join(c.opt.Dest, f.Path))] = true
	}
	backup := ""
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if os.IsNotExist(err) && path == root {
			return nil
		}
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if expected[path] && d.Type()&os.ModeSymlink == 0 {
			return nil
		}
		if backup == "" {
			backup, err = os.MkdirTemp(c.opt.Dest, ".stale-task-")
			if err != nil {
				return err
			}
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		target := filepath.Join(backup, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		return os.Rename(path, target)
	})
}

func (c *Client) downloadAll(files []protocol.FileInfo) bool {
	type result struct {
		fi protocol.FileInfo
		ok bool
	}
	results := make([]result, len(files))
	sem := make(chan struct{}, c.opt.Files)
	var wg sync.WaitGroup
	for i, fi := range files {
		wg.Add(1)
		go func(i int, fi protocol.FileInfo) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			dest := filepath.Join(c.opt.Dest, fi.Path)
			ok := c.downloadFileWithRetry(fi, dest)
			results[i] = result{fi: fi, ok: ok}
		}(i, fi)
	}
	wg.Wait()
	for _, r := range results {
		if !r.ok {
			return false
		}
	}
	return true
}

func (c *Client) downloadFileWithRetry(fi protocol.FileInfo, dest string) bool {
	name := filepath.Base(fi.Path)
	if fi.Size >= 1024*1024 {
		fmt.Printf("  %s (%.1f MB)\n", name, float64(fi.Size)/1024/1024)
	} else {
		fmt.Printf("  %s (%.0f KB)\n", name, float64(fi.Size)/1024)
	}
	var lastErr error
	for attempt := 1; attempt <= maxRetry; attempt++ {
		err := c.downloadFile(fi, dest)
		if err == nil {
			return true
		}
		lastErr = err
		fmt.Printf("    下载出错: %v (%d/%d)\n", err, attempt, maxRetry)
		if attempt < maxRetry {
			time.Sleep(2 * time.Second)
		}
		os.Remove(dest)
	}
	fmt.Printf("    ✗ 文件下载失败: %s (%v)\n", name, lastErr)
	return false
}

func (c *Client) downloadFile(fi protocol.FileInfo, dest string) (resultErr error) {
	if fi.SHA256 != "" {
		if fileMatchesHash(dest, fi) {
			c.doneBytes.Add(fi.Size)
			return nil
		}
		// 旧文件或断点未经确认；从头下载，避免拼接不同版本。
		if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if fi.SHA256 == "" {
		if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	credited := &atomic.Int64{}
	defer func() {
		if resultErr == nil && fi.SHA256 != "" && !fileMatchesHash(dest, fi) {
			resultErr = fmt.Errorf("文件内容校验失败：%s，请确认源文件未被修改", fi.Path)
			os.Remove(dest)
		}
		if resultErr != nil {
			c.doneBytes.Add(-credited.Load())
		}
	}()
	fileURL := c.opt.Server + "/files/" + url.PathEscape(fi.Path)
	os.MkdirAll(filepath.Dir(dest), 0755)
	if fi.Size == 0 {
		return os.WriteFile(dest, []byte{}, 0644)
	}
	if fi.Size >= minParallelSize {
		// 并行路径：预分配文件，多 goroutine 按 WriteAt 填充
		f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		if err := f.Truncate(fi.Size); err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		return c.downloadParallel(fileURL, dest, fi, credited)
	}
	return c.downloadSingle(fileURL, dest, fi, credited)
}

func fileMatchesHash(path string, fi protocol.FileInfo) bool {
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return err == nil && n == fi.Size && hex.EncodeToString(h.Sum(nil)) == fi.SHA256
}

type chunk struct{ start, end int64 }

func (c *Client) downloadParallel(fileURL, dest string, fi protocol.FileInfo, credited *atomic.Int64) error {
	var chunks []chunk
	for pos := int64(0); pos < fi.Size; {
		end := pos + c.opt.Chunk - 1
		if end >= fi.Size {
			end = fi.Size - 1
		}
		chunks = append(chunks, chunk{pos, end})
		pos = end + 1
	}
	failed := make(chan chunk, len(chunks))
	sem := make(chan struct{}, c.opt.Threads)
	var wg sync.WaitGroup
	for _, ck := range chunks {
		wg.Add(1)
		go func(ck chunk) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := c.fetchChunk(fileURL, dest, ck, credited); err != nil {
				failed <- ck
			}
		}(ck)
	}
	wg.Wait()
	close(failed)
	// 失败块串行补传
	for ck := range failed {
		if err := c.fetchChunk(fileURL, dest, ck, credited); err != nil {
			return fmt.Errorf("块[%d-%d]下载失败: %w", ck.start, ck.end, err)
		}
	}
	if st, err := os.Stat(dest); err != nil || st.Size() != fi.Size {
		if err == nil {
			return fmt.Errorf("大小不符: %d/%d", st.Size(), fi.Size)
		}
		return err
	}
	return nil
}

func backoffSleep(attempt int) {
	d := time.Duration(1<<attempt) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	time.Sleep(d)
}

func (c *Client) fetchChunk(fileURL, dest string, ck chunk, credited *atomic.Int64) error {
	expected := ck.end - ck.start + 1
	var lastErr error
	for attempt := 1; attempt <= maxRetry; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), chunkReqTimeout)
		req, _ := http.NewRequestWithContext(ctx, "GET", fileURL, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", ck.start, ck.end))
		resp, err := protocol.Shared.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			backoffSleep(attempt)
			continue
		}
		if resp.StatusCode == 200 && ck.start != 0 {
			resp.Body.Close()
			cancel()
			lastErr = fmt.Errorf("服务端不支持Range")
			backoffSleep(attempt)
			continue
		}
		if resp.StatusCode != 206 && resp.StatusCode != 200 {
			resp.Body.Close()
			cancel()
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			backoffSleep(attempt)
			continue
		}
		f, err := os.OpenFile(dest, os.O_WRONLY, 0644)
		if err != nil {
			resp.Body.Close()
			cancel()
			lastErr = err
			backoffSleep(attempt)
			continue
		}
		off := ck.start
		if resp.StatusCode == 200 {
			off = 0
		}
		buf := make([]byte, 256<<10)
		written := int64(0)
		var rerr error
		for {
			n, e := resp.Body.Read(buf)
			if n > 0 {
				if _, werr := f.WriteAt(buf[:n], off); werr != nil {
					rerr = werr
					break
				}
				off += int64(n)
				written += int64(n)
				c.doneBytes.Add(int64(n))
				credited.Add(int64(n))
			}
			if e == io.EOF {
				break
			}
			if e != nil {
				rerr = e
				break
			}
		}
		f.Close()
		resp.Body.Close()
		cancel()
		if rerr == nil && written == expected {
			return nil
		}
		c.doneBytes.Add(-written)
		credited.Add(-written)
		if rerr != nil {
			lastErr = rerr
		} else {
			lastErr = fmt.Errorf("块不完整: %d/%d", written, expected)
		}
		backoffSleep(attempt)
	}
	return lastErr
}

func (c *Client) downloadSingle(fileURL, dest string, fi protocol.FileInfo, credited *atomic.Int64) error {
	var existing int64
	if st, err := os.Stat(dest); err == nil {
		existing = st.Size()
	}
	if existing == fi.Size {
		c.doneBytes.Add(existing)
		credited.Add(existing)
		return nil
	}
	if existing > fi.Size {
		existing = 0
	}
	c.doneBytes.Add(existing)
	credited.Add(existing)
	var lastErr error
	for attempt := 1; attempt <= maxRetry; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), chunkReqTimeout)
		req, _ := http.NewRequestWithContext(ctx, "GET", fileURL, nil)
		appendMode := false
		if existing > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", existing))
			appendMode = true
		}
		resp, err := protocol.Shared.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			backoffSleep(attempt)
			continue
		}
		if resp.StatusCode == 206 && appendMode {
			// 追加续传
		} else if resp.StatusCode == 200 {
			appendMode = false
		} else {
			resp.Body.Close()
			cancel()
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			backoffSleep(attempt)
			continue
		}
		flag := os.O_WRONLY | os.O_CREATE
		if appendMode {
			flag |= os.O_APPEND
		}
		f, err := os.OpenFile(dest, flag, 0644)
		if err != nil {
			resp.Body.Close()
			cancel()
			lastErr = err
			backoffSleep(attempt)
			continue
		}
		if !appendMode {
			c.doneBytes.Add(-credited.Load())
			credited.Store(0)
			f.Truncate(0)
		}
		buf := make([]byte, 256<<10)
		var rerr error
		for {
			n, e := resp.Body.Read(buf)
			if n > 0 {
				if _, werr := f.Write(buf[:n]); werr != nil {
					rerr = werr
					break
				}
				c.doneBytes.Add(int64(n))
				credited.Add(int64(n))
			}
			if e == io.EOF {
				break
			}
			if e != nil {
				rerr = e
				break
			}
		}
		f.Close()
		resp.Body.Close()
		cancel()
		if rerr == nil {
			if st, err := os.Stat(dest); err == nil && st.Size() == fi.Size {
				return nil
			}
			lastErr = fmt.Errorf("大小不符")
		} else {
			lastErr = rerr
		}
		c.doneBytes.Add(-credited.Load())
		credited.Store(0)
		existing = 0
		backoffSleep(attempt)
	}
	return lastErr
}

// ---- 上传 ----

func (c *Client) uploadFolder(server, folder, originalName, claimToken string) bool {
	folderPath := filepath.Join(c.opt.Dest, folder)
	up := uploader.New(uploader.Config{
		LogDir:     filepath.Join(c.opt.Dest, "logs"),
		ServerURL:  server,
		WorkerID:   c.workerID,
		TaskID:     folder,
		MaxRetries: c.opt.UploadRetries,
		OutputDir:  filepath.Join("/opt/lm_agent/output", filepath.Dir(folder), claimToken),
	})
	report := func(fname, status, message string) {
		protocol.JSONRequest("POST", server+"/api/upload_status",
			protocol.UploadStatusReq{Folder: folder, Status: status, Message: message},
			map[string]string{"X-Worker-Id": c.workerID}, 30*time.Second, nil)
	}
	progress := func(phase string, compressPct, uploadPct, retryCount int) {
		c.updateProg(phase, compressPct, uploadPct, retryCount)
	}

	c.setProg(folder, protocol.PhaseCompressing, 100, 0, 0, 0)
	fmt.Println("  [上传] 开始上传到吉翼数采平台...")
	ok, msg := up.UploadFolder(folderPath, originalName, report, progress)
	if ok {
		fmt.Printf("  [上传] %s 上传成功 ✓\n\n", folder)
	} else {
		fmt.Printf("  [上传] %s 上传失败: %s\n\n", folder, msg)
	}
	return ok
}

// ---- 工具 ----

func gb(n int64) float64 { return float64(n) / 1024 / 1024 / 1024 }

func freeBytes(path string) uint64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0
	}
	return st.Bavail * uint64(st.Bsize)
}
