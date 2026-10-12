// Package update 查询稳定版 GitHub Release；仅检查和提供下载链接，不修改运行中的程序。
package update

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"fenpei/internal/version"
)

// Repository 可由构建脚本写入，格式为 owner/repo。环境变量优先。
var Repository = "zhu-hang131433/fenpei"
var repoPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9_.-]+$`)
var versionPattern = regexp.MustCompile(`^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

type Status struct {
	Current     string `json:"current_version"`
	Latest      string `json:"latest_version,omitempty"`
	Repository  string `json:"repository,omitempty"`
	Available   bool   `json:"available"`
	Checking    bool   `json:"checking"`
	CheckedAt   string `json:"checked_at,omitempty"`
	ReleaseURL  string `json:"release_url,omitempty"`
	DownloadURL string `json:"download_url,omitempty"`
	Notes       string `json:"notes,omitempty"`
	Error       string `json:"error,omitempty"`
}

type Checker struct {
	mu                     sync.Mutex
	repo, current, apiBase string
	client                 *http.Client
	relay                  func() string
	state                  Status
	attempted              time.Time
	interval               time.Duration
}

func NewConfigured(relay func() string) *Checker {
	repo := os.Getenv("FENPEI_UPDATE_REPO")
	if repo == "" {
		repo = Repository
	}
	return New(repo, version.Version, relay)
}
func New(repo, current string, relay func() string) *Checker {
	return &Checker{repo: repo, current: current, apiBase: "https://api.github.com", client: &http.Client{Timeout: 12 * time.Second}, relay: relay, interval: time.Hour, state: Status{Current: current, Repository: repo}}
}

// Status 在首次访问及缓存过期时异步检查，页面请求不等待外部网络。
// 手动检查仍至少间隔 30 秒，避免点击和多页面耗尽 GitHub 限额。
func (c *Checker) Status(force bool) Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	age := time.Since(c.attempted)
	if !c.state.Checking && (c.attempted.IsZero() || age >= c.interval || force && age >= 30*time.Second) {
		c.attempted = time.Now()
		c.state.Checking = true
		go c.check()
	}
	return c.state
}
func (c *Checker) check() {
	result, err := c.fetch()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Checking = false
	c.interval = time.Hour
	if err != nil {
		c.state.Error = err.Error()
		c.interval = 5 * time.Minute
		return
	}
	result.Current = c.current
	result.Available = newer(result.Latest, c.current)
	if result.Error != "" {
		c.interval = 5 * time.Minute
	}
	if result.Checking {
		c.interval = 10 * time.Second
		result.Checking = false
		result.Error = "服务端正在检查更新，稍后自动刷新"
	}
	c.state = result
}
func (c *Checker) fetch() (Status, error) {
	if c.relay != nil {
		if base := c.relay(); base != "" {
			var remote Status
			if err := c.get(strings.TrimRight(base, "/")+"/api/update", &remote); err == nil && remote.Repository != "" && (c.repo == "" || c.repo == remote.Repository) {
				if err := validateLinks(remote); err == nil {
					return remote, nil
				}
			}
		}
	}
	state := Status{Current: c.current, Repository: c.repo}
	if !repoPattern.MatchString(c.repo) || strings.HasSuffix(c.repo, "/.") || strings.HasSuffix(c.repo, "/..") {
		return state, fmt.Errorf("尚未配置有效的 GitHub 更新仓库")
	}
	var release struct {
		Tag        string `json:"tag_name"`
		URL        string `json:"html_url"`
		Body       string `json:"body"`
		Draft      bool   `json:"draft"`
		Prerelease bool   `json:"prerelease"`
		Assets     []struct {
			Name  string `json:"name"`
			URL   string `json:"browser_download_url"`
			State string `json:"state"`
		} `json:"assets"`
	}
	if err := c.get(c.apiBase+"/repos/"+c.repo+"/releases/latest", &release); err != nil {
		return state, err
	}
	if release.Draft || release.Prerelease || !versionPattern.MatchString(release.Tag) {
		return state, fmt.Errorf("发布版本格式无效，需使用 v主版本.次版本.补丁版本")
	}
	state.Latest = strings.TrimPrefix(release.Tag, "v")
	state.ReleaseURL = release.URL
	state.Notes = release.Body
	state.CheckedAt = time.Now().Format(time.RFC3339)
	expected := "fenpei_" + state.Latest + "-1_amd64.deb"
	for _, asset := range release.Assets {
		if asset.Name == expected && asset.State == "uploaded" {
			state.DownloadURL = asset.URL
			break
		}
	}
	if err := validateLinks(state); err != nil {
		return state, err
	}
	return state, nil
}
func (c *Checker) get(address string, out any) error {
	req, err := http.NewRequest("GET", address, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "FenPei/"+c.current)
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("无法检查更新，请检查网络连接")
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return fmt.Errorf("更新仓库不可访问或尚未发布正式版本")
	}
	if resp.StatusCode == 403 || resp.StatusCode == 429 {
		return fmt.Errorf("更新检查暂时受限，稍后自动重试")
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("更新服务返回 HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out); err != nil {
		return fmt.Errorf("更新服务响应无效")
	}
	return nil
}
func validateLinks(s Status) error {
	if !repoPattern.MatchString(s.Repository) {
		return fmt.Errorf("更新仓库格式无效")
	}
	for _, link := range []string{s.ReleaseURL, s.DownloadURL} {
		if link == "" {
			continue
		}
		u, err := url.Parse(link)
		if err != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil || !strings.HasPrefix(u.Path, "/"+s.Repository+"/releases/") {
			return fmt.Errorf("更新下载链接无效")
		}
	}
	return nil
}
func newer(latest, current string) bool {
	a, b := versionPattern.FindStringSubmatch(latest), versionPattern.FindStringSubmatch(current)
	if a == nil || b == nil {
		return false
	}
	for i := 1; i <= 3; i++ {
		x, errX := strconv.ParseUint(a[i], 10, 64)
		y, errY := strconv.ParseUint(b[i], 10, 64)
		if errX != nil || errY != nil {
			return false
		}
		if x != y {
			return x > y
		}
	}
	return false
}
func (c *Checker) Handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(c.Status(r.URL.Query().Get("force") == "1"))
}
