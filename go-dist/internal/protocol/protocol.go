// Package protocol 定义服务端/客户端共享的 API 路径、数据结构与 HTTP 工具。
package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

// TimeFormat 与 Python 版一致的时间格式
const TimeFormat = "2006-01-02 15:04:05"

// Now 返回本地时间字符串
func Now() string { return time.Now().Format(TimeFormat) }

// HumanSize 人类可读大小
func HumanSize(n float64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	i := 0
	for n >= 1024 && i < len(units)-1 {
		n /= 1024
		i++
	}
	return fmt.Sprintf("%.1f%s", n, units[i])
}

// ---- 文件夹状态 ----
const (
	StatusPending    = "pending"
	StatusInProgress = "in_progress"
	StatusCompleted  = "completed"
)

// ---- 实时阶段 ----
const (
	PhaseDownloading = "downloading"
	PhaseCompressing = "compressing"
	PhaseUploading   = "uploading"
	PhaseRetrying    = "retrying"
)

// ---- API 路径（与 Python 版保持一致，便于混部迁移）----
const (
	PathStatusPage  = "/"
	PathAPIStatus   = "/api/status"
	PathRegister    = "/api/register"
	PathNext        = "/api/next"
	PathComplete    = "/api/complete"
	PathProgress    = "/api/progress"
	PathHeartbeat   = "/api/heartbeat"
	PathUploadStat  = "/api/upload_status"
	PathSkip        = "/api/skip"
	PathMarkUpload  = "/api/mark_uploaded"
	PathPause       = "/api/pause"
	PathBatchSkip   = "/api/batch_skip"
	PathFolderUpSta = "/api/folder_upload_status"
	PathFolderProg  = "/api/folder_progress"
	PrefixFiles     = "/files/"
)

// ---- 共享数据结构 ----

type FileInfo struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256,omitempty"`
}

type RegisterResp struct {
	WorkerID string `json:"worker_id"`
	Label    string `json:"label"`
	IP       string `json:"ip"`
}

type NextResp struct {
	ClaimToken    string     `json:"claim_token,omitempty"`
	Waiting       bool       `json:"waiting,omitempty"`
	RetryAfter    int        `json:"retry_after,omitempty"`
	FolderName    string     `json:"folder_name,omitempty"`
	Done          bool       `json:"done"`
	Message       string     `json:"message,omitempty"`
	Folder        string     `json:"folder,omitempty"`
	FileCount     int        `json:"file_count,omitempty"`
	TotalSize     int64      `json:"total_size,omitempty"`
	MaxFolderSize int64      `json:"max_folder_size,omitempty"`
	Files         []FileInfo `json:"files,omitempty"`
}

type CompleteReq struct {
	ClaimToken string `json:"claim_token,omitempty"`
	Folder     string `json:"folder"`
}

type ProgressReq struct {
	File  string `json:"file"`
	Bytes int64  `json:"bytes"`
}

type FolderProgressReq struct {
	Folder      string `json:"folder"`
	Phase       string `json:"phase,omitempty"`
	DownloadPct int    `json:"download_pct,omitempty"`
	CompressPct int    `json:"compress_pct,omitempty"`
	UploadPct   int    `json:"upload_pct,omitempty"`
	RetryCount  int    `json:"retry_count,omitempty"`
}

type UploadStatusReq struct {
	Folder  string `json:"folder"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type SkipReq struct {
	Folder string `json:"folder"`
	Skip   bool   `json:"skip"`
}

type BatchSkipReq struct {
	Folders []string `json:"folders"`
	Skip    bool     `json:"skip"`
}

type PauseReq struct {
	Paused bool `json:"paused"`
}

type FolderUploadStatusResp struct {
	UploadStatus string `json:"upload_status"`
	Status       string `json:"status"`
	Skip         bool   `json:"skip"`
}

type SimpleResp struct {
	OK      bool   `json:"ok,omitempty"`
	Success bool   `json:"success,omitempty"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
	Folder  string `json:"folder,omitempty"`
	Skip    bool   `json:"skip,omitempty"`
	Count   int    `json:"count,omitempty"`
}

// Shared 供所有组件复用的 HTTP 客户端（连接池高并发友好）
var Shared = &http.Client{
	Transport: &http.Transport{
		Proxy: func(req *http.Request) (*url.URL, error) {
			ip := net.ParseIP(req.URL.Hostname())
			if ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()) {
				return nil, nil
			}
			return http.ProxyFromEnvironment(req)
		},
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	},
}

// JSONRequest 发起 HTTP 请求（可选 JSON body），解码响应到 out（可为 nil）。
func JSONRequest(method, url string, body any, header map[string]string, timeout time.Duration, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	ctx := context.Background()
	var cancel context.CancelFunc = func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := Shared.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		s := string(data)
		if len(s) > 200 {
			s = s[:200]
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, s)
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}
