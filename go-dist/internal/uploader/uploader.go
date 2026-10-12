package uploader

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fenpei/internal/protocol"
)

// Config 上传器配置
type Config struct {
	MaxRetries int    // 首次尝试外的最大重试次数；默认 5，最大 20
	TaskID     string // 池中任务 ID；与用于平台匹配的原始文件夹名区分
	LogDir     string // 客户端本地日志目录（upload_YYYYMMDD.log）
	ServerURL  string // 分发服务器地址（用于上报状态/查询跳过标记，可为空）
	WorkerID   string // 分发 worker id
	OutputDir  string // 压缩包输出目录，默认 /opt/lm_agent/output
}

type Uploader struct {
	cfg     Config
	session uploadSession
}

// Reporter 上报上传状态到分发服务器（folder, status, message）
type Reporter func(folder, status, message string)

// ProgressFn 实时进度回调：phase 为空串表示不变，pct 传 -1 表示不变
type ProgressFn func(phase string, compressPct, uploadPct, retryCount int)

func New(cfg Config) *Uploader {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.MaxRetries > MaximumMaxRetries {
		cfg.MaxRetries = MaximumMaxRetries
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "/opt/lm_agent/output"
	}
	return &Uploader{cfg: cfg}
}

// UploadFolder 上传流程采用有限重试、指数退避及随机错峰。
func (u *Uploader) UploadFolder(folderPath, folderName string, report Reporter, progress ProgressFn) (bool, string) {
	u.session = uploadSession{}
	taskID := u.cfg.TaskID
	if taskID == "" {
		taskID = folderName
	}
	return u.runUpload(folderName, report, progress, func(retry int) (bool, string, string) {
		step := ""
		if progress != nil {
			progress("", -1, -1, retry)
		}
		ok, msg := u.doUpload(folderPath, folderName, report, progress, retry, &step)
		return ok, msg, step
	}, func(delay time.Duration) bool { return waitWithServerCheck(u.cfg.ServerURL, taskID, delay, report) })
}

func (u *Uploader) doUpload(folderPath, folderName string, report Reporter, progress ProgressFn, retry int, step *string) (bool, string) {
	if retry > 0 {
		fmt.Printf("  [上传] === 第 %d 次重试 ===\n", retry)
	} else {
		fmt.Println("  [上传] === 首次上传 ===")
	}
	fail := func(st, msg string) (bool, string) {
		*step = st
		fmt.Printf("  [上传] %s\n", msg)
		u.logf("✗ 上传失败 | 文件夹: %s | 原因: %s", folderName, msg)
		if report != nil {
			report(folderName, st, msg)
		}
		return false, msg
	}

	if u.session.FolderPath != folderPath || u.session.FolderName != folderName {
		u.session = uploadSession{FolderPath: folderPath, FolderName: folderName}
	}
	session := &u.session
	// 1. 推导账号并登录
	account := DeriveAccount(folderName)
	if account == "" {
		return fail("no_jz", fmt.Sprintf("无法从文件夹名提取 JZ 编号: %s", folderName))
	}
	fmt.Printf("  [上传] 推导账号: %s\n", account)

	if session.Platform == nil {
		session.Platform = NewPlatform()
	}
	p := session.Platform
	if p.token == "" || p.username != account {
		fmt.Printf("  [上传] 登录中... (账号: %s)\n", account)
		if err := p.Login(account); err != nil {
			return fail("login_failed", fmt.Sprintf("登录失败: %s (%v)", account, err))
		}
	}

	// 2. 使用文件夹末尾八位日期编号匹配当前账号任务
	taskName := folderName
	fmt.Printf("  [上传] 文件夹名: %s\n", folderName)
	fmt.Printf("  [上传] 匹配日期: %s\n", TaskDateSuffix(taskName))

	// 3. 查找匹配任务
	if session.TaskID == "" {
		id, item, err := p.FindMatchingTask(taskName)
		if err != nil {
			return fail("no_task", "查询匹配任务失败: "+err.Error())
		}
		if id == "" {
			return fail("no_task", fmt.Sprintf("未找到匹配任务: %s", taskName))
		}
		session.TaskID = id
		session.TaskItem = item
		switch mstr(item["status"]) {
		case "completed", "已完成":
			session.StartDone = true
			session.CollectionDone = true
		case "in_progress", "collecting", "进行中", "采集中":
			session.StartDone = true
		}
	}
	taskID, taskItem := session.TaskID, session.TaskItem
	// 已确认成功的心跳和任务操作不因后续失败而重复。
	if !session.HealthDone {
		if err := p.ReportHealth("running", "running", 0); err != nil {
			return fail("heartbeat_failed", "心跳上报失败: "+err.Error())
		}
		session.HealthDone = true
	}
	if !session.StartDone {
		if err := p.StartTask(taskID); err != nil {
			return fail("start_failed", "开始任务失败: "+err.Error())
		}
		session.StartDone = true
	}
	if !session.CollectionDone {
		if !session.IdleSent {
			p.ReportHealth("idle", "idle", 0)
			session.IdleSent = true
		}
		if err := p.CompleteCollection(taskID, map[string]any{"actualCount": 0, "collectStatus": "completed"}); err != nil {
			return fail("complete_failed", "结束任务失败: "+err.Error())
		}
		session.CollectionDone = true
	}

	// 7. 获取任务信息
	if session.Info == nil {
		info, err := p.GetTaskInfo(taskID)
		if err != nil {
			return fail("info_failed", "获取任务信息失败: "+err.Error())
		}
		session.Info = info
	}
	info := session.Info
	remotePath := mstr(info["remotePath"])
	fmt.Printf("  [上传] 远程路径: %s\n", remotePath)

	// 8. 压缩文件夹（并行压缩；存在完整压缩包则复用，避免重量失败后重复压缩）
	outDir := u.cfg.OutputDir
	if err := os.MkdirAll(outDir, 0755); err != nil {
		outDir = filepath.Join(os.TempDir(), "fenpei-output", filepath.Dir(u.cfg.TaskID))
		if err := os.MkdirAll(outDir, 0755); err != nil {
			return fail("compress_failed", "无法创建压缩目录: "+err.Error())
		}
	}
	zipPath := filepath.Join(outDir, folderName+".zip")
	if _, err := os.Stat(zipPath); err == nil {
		fmt.Printf("  [压缩] 复用已有压缩包: %s\n", zipPath)
		if progress != nil {
			progress("compressing", 100, -1, -1)
		}
	} else {
		if progress != nil {
			progress("compressing", 0, -1, -1)
		}
		err := ZipFolder(folderPath, zipPath, func(pct int) {
			if progress != nil {
				progress("compressing", pct, -1, -1)
			}
		})
		if err != nil {
			os.Remove(zipPath)
			return fail("compress_failed", "压缩失败: "+err.Error())
		}
		if st, err := os.Stat(zipPath); err == nil {
			fmt.Printf("  [压缩] 完成: %s (%s)\n", zipPath, protocol.HumanSize(float64(st.Size())))
		}
	}

	// 9. MinIO 上传
	if !session.ObjectUploaded && session.DAG == nil {
		dag, err := p.GetDagClientInfo()
		if err != nil {
			return fail("upload_failed", "获取MinIO配置失败: "+err.Error())
		}
		session.DAG = dag
	}
	dag := session.DAG
	oss := mapOf(mapOf(dag["retData"])["oss"])
	endpoint := envOr("MINIO_ADDRESS", DefaultEndpoint)
	bucket := envOr("MINIO_BUCKET", mstr(oss["bucket"]))
	accessKey := mstr(oss["accessKey"])
	secretKey := mstr(oss["secretKey"])
	ossPath := mstr(oss["path"])
	uploadPath := mstr(oss["uploadPath"])

	// 对象键清理逻辑（与 Python service.upload_file 一致）
	fullPrefix := strings.Trim(ossPath, "/")
	if uploadPath != "" {
		up := strings.Trim(uploadPath, "/")
		if fullPrefix != "" {
			fullPrefix = fullPrefix + "/" + up
		} else {
			fullPrefix = up
		}
	} else if fullPrefix == "" {
		fullPrefix = "task_data"
	}
	fullPrefix = "/" + strings.Trim(fullPrefix, "/")

	clean := remotePath
	upTrim := strings.Trim(uploadPath, "/")
	switch {
	case strings.HasPrefix(clean, fullPrefix):
		clean = clean[len(fullPrefix):]
	case upTrim != "" && strings.HasPrefix(clean, upTrim+"/"):
		clean = clean[len(upTrim)+1:]
	case upTrim != "" && strings.HasPrefix(clean, upTrim):
		clean = strings.TrimLeft(clean[len(upTrim):], "/")
	}
	if clean == "" {
		clean = filepath.Base(zipPath)
	}
	fullObject := strings.TrimPrefix(strings.ReplaceAll(fullPrefix+clean, "//", "/"), "/")
	if !session.ObjectUploaded {
		fmt.Printf("  [上传] 开始上传到 MinIO: %s (bucket=%s endpoint=%s)\n", fullObject, bucket, endpoint)
		if progress != nil {
			progress("uploading", 100, 0, -1)
		}
		mc := NewMinio(endpoint, accessKey, secretKey)
		if !session.BucketChecked {
			if err := mc.EnsureBucket(bucket); err != nil {
				if strings.Contains(err.Error(), "HTTP 403") {
					session.DAG = nil
				}
				return fail("upload_failed", "MinIO bucket 检查失败: "+err.Error())
			}
			session.BucketChecked = true
		}
		err := mc.PutObjectFile(bucket, fullObject, zipPath, func(pct int) {
			if progress != nil && pct >= 0 {
				progress("uploading", -1, pct, -1)
			}
		})
		if err != nil {
			if strings.Contains(err.Error(), "HTTP 403") {
				session.DAG = nil
				session.BucketChecked = false
			}
			return fail("upload_failed", "MinIO 上传失败: "+err.Error())
		}
		session.ObjectUploaded = true
		fmt.Println("  [上传] MinIO 上传完成，后续通知失败不会重传 ZIP")
	}
	storagePath := clean

	// 10. 上传状态通知；通知失败只重试此步骤。
	if progress != nil {
		progress("notifying", -1, 100, -1)
	}
	if err := p.DataUploadStates(taskID, map[string]any{
		"uploadStatus": "uploaded",
		"storagePath":  storagePath,
		"errorCode":    "0",
		"errorMsg":     "",
	}); err != nil {
		return fail("state_notify_failed", "上传状态通知失败: "+err.Error())
	}
	fmt.Println("  [上传] 上传状态通知成功")

	// 11. 清理压缩包
	os.Remove(zipPath)
	fmt.Println("  [上传] 已清理临时压缩文件")

	// 12. 写客户端日志
	taskJSON, _ := json.Marshal(taskItem)
	u.logf("✓ 上传成功 | 时间: %s | 文件夹: %s | 任务名: %s | 任务ID: %s | 账号: %s | 存储路径: %s | 任务信息: %s",
		time.Now().Format("2006-01-02 15:04:05"), folderName, taskName, taskID, account, storagePath, string(taskJSON))
	fmt.Printf("  [日志] 已记录到 %s\n", u.todayLogPath())

	// 13. 删除已上传的文件夹
	if err := moveToTrash(folderPath); err != nil {
		fmt.Printf("  [清理] 删除文件夹失败（非致命）: %v\n", err)
	} else {
		fmt.Printf("  [清理] 已删除文件夹: %s\n", folderName)
	}

	// 14. 上报服务器
	if report != nil {
		report(folderName, "success", fmt.Sprintf("任务ID:%s 账号:%s 路径:%s", taskID, account, storagePath))
	}
	*step = ""
	return true, fmt.Sprintf("上传完成: 任务ID=%s 账号=%s", taskID, account)
}

// ---- 服务端联动 ----

func getFolderUploadStatus(serverURL, folder string) (*protocol.FolderUploadStatusResp, error) {
	var out protocol.FolderUploadStatusResp
	u := fmt.Sprintf("%s/api/folder_upload_status?folder=%s", strings.TrimRight(serverURL, "/"), url.QueryEscape(folder))
	if err := protocol.JSONRequest("GET", u, nil, nil, 10*time.Second, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// waitWithServerCheck 等待期间每 30s 检查服务端状态；
// 若已标记 success/skip 则返回 true（跳过本地上传）。
func waitWithServerCheck(serverURL, folder string, waitTime time.Duration, report Reporter) bool {
	waited := time.Duration(0)
	for waited < waitTime {
		d := 30 * time.Second
		if waitTime-waited < d {
			d = waitTime - waited
		}
		time.Sleep(d)
		waited += d
		if serverURL == "" {
			continue
		}
		if info, err := getFolderUploadStatus(serverURL, folder); err == nil && info != nil {
			if info.UploadStatus == "success" || info.Skip {
				fmt.Printf("  [上传] 服务端已标记（upload_status=%s skip=%v），跳过本地上传\n", info.UploadStatus, info.Skip)
				if report != nil {
					report(folder, "skipped_by_server", "服务端已标记，停止本地上传重试")
				}
				return true
			}
		}
	}
	return false
}

// ---- 工具 ----

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (u *Uploader) todayLogPath() string {
	return filepath.Join(u.cfg.LogDir, "upload_"+time.Now().Format("20060102")+".log")
}

func (u *Uploader) logf(format string, args ...any) {
	os.MkdirAll(u.cfg.LogDir, 0755)
	f, err := os.OpenFile(u.todayLogPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("  [日志] 写入失败: %v", err)
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "[%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), fmt.Sprintf(format, args...))
}

func moveToTrash(folderPath string) error {
	name := filepath.Base(folderPath)
	trashDir := filepath.Join(filepath.Dir(folderPath), ".trash")
	if err := os.MkdirAll(trashDir, 0755); err != nil {
		return err
	}
	trashPath := filepath.Join(trashDir, name)
	if _, err := os.Stat(trashPath); err == nil {
		if err := os.RemoveAll(trashPath); err != nil {
			return err
		}
	}
	if err := os.Rename(folderPath, trashPath); err != nil {
		return err
	}
	return os.RemoveAll(trashPath)
}
