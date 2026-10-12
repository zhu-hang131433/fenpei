// Package uploader 吉翼数采平台自动上传（auto_upload.py 的 Go 移植）。
//
// 流程：推导账号登录 → 文件夹名转任务名 → 查找匹配任务 → 心跳 →
// 开始/结束任务 → 获取 remotePath → 并行压缩 → MinIO(S3) 上传 → 状态通知 →
// 清理压缩包/文件夹 → 写日志。
package uploader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

// 平台固定配置（与 lm_agent config.py 一致），可用环境变量覆盖：
//
//	FENPEI_BASE_URL  平台地址
//	MINIO_ADDRESS    MinIO endpoint（host:port）
//	MINIO_BUCKET     MinIO bucket
const (
	DefaultBaseURL  = "http://10.5.66.51"
	DefaultEndpoint = "10.5.66.51:9002"
)

var jzRe = regexp.MustCompile(`JZ(\d+)`)
var taskSuffixRe = regexp.MustCompile(`(?:^|[^0-9])([0-9]{8})$`)

// TaskDateSuffix 提取名称末尾的八位日期编号，不匹配名称中间的日期。
func TaskDateSuffix(name string) string {
	m := taskSuffixRe.FindStringSubmatch(strings.TrimSpace(name))
	if len(m) == 2 {
		return m[1]
	}
	return ""
}

// ---- 名称转换（与 Python 版一致）----

// FolderNameToTaskName 文件夹名转任务名：
// (捷翼工厂)JZ08-热缩（...）-20260921 → (捷翼工厂)JZ08/热缩.../20260921
func FolderNameToTaskName(folderName string) string {
	name := strings.ReplaceAll(folderName, "（", "")
	name = strings.ReplaceAll(name, "）", "")
	name = strings.ReplaceAll(name, "-", "/")
	return name
}

// ExtractJZNumber 提取 JZ 编号
func ExtractJZNumber(folderName string) string {
	m := jzRe.FindStringSubmatch(folderName)
	if len(m) >= 2 {
		return m[1]
	}
	return ""
}

// DeriveAccount 根据文件夹名推导账号：JZ08 → JIANZHIEGO08
func DeriveAccount(folderName string) string {
	jz := ExtractJZNumber(folderName)
	if jz == "" {
		return ""
	}
	return "JIANZHIEGO" + jz
}

// ---- 平台 API 客户端 ----

type Platform struct {
	baseURL   string
	token     string
	tokenType string
	username  string
	userID    string
	hc        *http.Client
}

func NewPlatform() *Platform {
	base := os.Getenv("FENPEI_BASE_URL")
	if base == "" {
		base = DefaultBaseURL
	}
	return &Platform{
		baseURL: strings.TrimRight(base, "/"),
		hc:      &http.Client{Timeout: 60 * time.Second},
	}
}

func (p *Platform) authHeader() map[string]string {
	if p.token == "" {
		return nil
	}
	return map[string]string{"authorization": p.tokenType + " " + p.token}
}

func (p *Platform) call(method, path string, body any, extra map[string]string) (map[string]any, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, p.baseURL+path, rd)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range p.authHeader() {
		req.Header.Set(k, v)
	}
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	resp, err := p.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求异常: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		if resp.StatusCode == 401 || resp.StatusCode == 403 {
			p.token = ""
			p.username = ""
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncStr(string(data), 200))
	}
	var out map[string]any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func truncStr(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// ---- map 工具 ----

func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func mstr(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		if t == float64(int64(t)) {
			return fmt.Sprintf("%d", int64(t))
		}
		return fmt.Sprintf("%v", t)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

func mint(v any) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case string:
		var n int
		fmt.Sscanf(t, "%d", &n)
		return n
	}
	return 0
}

// ---- API 方法 ----

// Login 登录（retCode == 0 表示成功）
func (p *Platform) Login(account string) error {
	password, err := privateSetting("FENPEI_PLATFORM_PASSWORD", "platform_password")
	if err != nil {
		return err
	}
	r, err := p.call("POST", "/cab/platform/user/login",
		map[string]string{"accountName": account, "password": password}, nil)
	if err != nil {
		return err
	}
	if mint(r["retCode"]) != 0 {
		return fmt.Errorf("retCode=%v retMsg=%v", r["retCode"], r["retMsg"])
	}
	rd := mapOf(r["retData"])
	p.token = mstr(rd["token"])
	p.tokenType = mstr(rd["tokenType"])
	p.userID = mstr(rd["userId"])
	p.username = account
	return nil
}

// GetTaskList 分页获取任务列表
func (p *Platform) GetTaskList(current, size int) ([]any, int, error) {
	r, err := p.call("POST", "/embodied/collection-tasks/list",
		map[string]any{"current": current, "size": size}, nil)
	if err != nil {
		return nil, 0, err
	}
	if mint(r["retCode"]) != 200 {
		return nil, 0, fmt.Errorf("retCode=%v retMsg=%v", r["retCode"], r["retMsg"])
	}
	rd := mapOf(r["retData"])
	records, ok := rd["records"].([]any)
	pages := mint(rd["pages"])
	if !ok || pages < 0 || (len(records) > 0 && pages < current) {
		return nil, 0, fmt.Errorf("任务列表分页字段异常，无法确认所有候选任务")
	}
	return records, pages, nil
}

// FindMatchingTask 在当前登录账号的任务中按末尾八位日期编号唯一匹配。
func (p *Platform) FindMatchingTask(taskName string) (string, map[string]any, error) {
	suffix := TaskDateSuffix(taskName)
	if suffix == "" {
		return "", nil, fmt.Errorf("名称末尾缺少八位日期编号: %s", taskName)
	}
	var matchedID string
	var matched map[string]any
	current := 1
	for {
		records, pages, err := p.GetTaskList(current, 50)
		if err != nil {
			return "", nil, fmt.Errorf("获取任务列表失败: %w", err)
		}
		if len(records) == 0 {
			break
		}
		for _, it := range records {
			item := mapOf(it)
			if TaskDateSuffix(mstr(item["collectionTaskName"])) == suffix {
				id := mstr(item["id"])
				if id == "" {
					return "", nil, fmt.Errorf("匹配任务缺少任务 ID: %s", suffix)
				}
				if matchedID != "" && matchedID != id {
					return "", nil, fmt.Errorf("当前账号有多个任务的日期编号为 %s，无法唯一匹配（ID=%s、%s），请清理旧任务或修改日期编号", suffix, matchedID, id)
				}
				matchedID, matched = id, item
			}
		}
		if current >= pages {
			break
		}
		current++
	}
	return matchedID, matched, nil
}

// GetTaskInfo 获取任务信息（返回 retData）
func (p *Platform) GetTaskInfo(taskID string) (map[string]any, error) {
	r, err := p.call("GET", "/embodied/collection-tasks/"+taskID, nil, nil)
	if err != nil {
		return nil, err
	}
	if mint(r["retCode"]) != 200 {
		return nil, fmt.Errorf("retCode=%v retMsg=%v", r["retCode"], r["retMsg"])
	}
	return mapOf(r["retData"]), nil
}

// StartTask 开始任务
func (p *Platform) StartTask(taskID string) error {
	r, err := p.call("PUT", "/embodied/collection-tasks/"+taskID+"/start", nil, nil)
	if err != nil {
		return err
	}
	if mint(r["retCode"]) != 200 {
		return fmt.Errorf("retCode=%v retMsg=%v", r["retCode"], r["retMsg"])
	}
	return nil
}

// CompleteCollection 结束任务
func (p *Platform) CompleteCollection(taskID string, payload map[string]any) error {
	r, err := p.call("PUT", "/embodied/collection-tasks/"+taskID+"/complete", payload, nil)
	if err != nil {
		return err
	}
	if mint(r["retCode"]) != 200 {
		return fmt.Errorf("retCode=%v retMsg=%v", r["retCode"], r["retMsg"])
	}
	return nil
}

// ReportHealth 心跳上报
func (p *Platform) ReportHealth(robotStatus, agentStatus string, errCode int) error {
	if p.token == "" {
		return fmt.Errorf("未登录")
	}
	payload := map[string]any{
		"robotStatus":    robotStatus,
		"agentStatus":    agentStatus,
		"robotErrorCode": errCode,
		"agentErrorCode": errCode,
		"robotErrorMsg":  "",
		"agentErrorMsg":  "",
		"timestamp":      time.Now().Format("2006-01-02T15:04:05"),
	}
	r, err := p.call("PUT", "/embodied/robots/health", payload, nil)
	if err != nil {
		return err
	}
	if mint(r["retCode"]) != 200 {
		return fmt.Errorf("retCode=%v retMsg=%v", r["retCode"], r["retMsg"])
	}
	return nil
}

// DataUploadStates 上传状态通知
func (p *Platform) DataUploadStates(taskID string, payload map[string]any) error {
	r, err := p.call("POST", "/embodied/collection-tasks/"+taskID+"/upload", payload, nil)
	if err != nil {
		return err
	}
	if mint(r["retCode"]) != 200 {
		return fmt.Errorf("retCode=%v retMsg=%v", r["retCode"], r["retMsg"])
	}
	return nil
}

// GetDagClientInfo 获取 DAG 客户端信息（含 MinIO oss 配置，静态 ac-token）
func (p *Platform) GetDagClientInfo() (map[string]any, error) {
	token, err := privateSetting("FENPEI_DAG_ACCESS_TOKEN", "dag_access_token")
	if err != nil {
		return nil, err
	}
	r, err := p.call("GET", "/embodied/dag-client/info", nil,
		map[string]string{"ac-token": token})
	if err != nil {
		return nil, err
	}
	return r, nil
}
