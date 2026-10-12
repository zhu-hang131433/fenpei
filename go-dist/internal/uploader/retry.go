package uploader

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"
)

const (
	DefaultMaxRetries = 5
	MaximumMaxRetries = 20
	retryBaseDelay    = 30 * time.Second
	retryMaxDelay     = 10 * time.Minute
	initialMaxDelay   = 15 * time.Second
)

// crypto/rand 使不同电脑和同时启动的进程无需共享随机种子。
func randomDuration(low, high time.Duration) time.Duration {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(high-low)+1))
	if err != nil {
		return high
	} // 随机源故障时仍保持有限等待，避免紧密重试。
	return low + time.Duration(n.Int64())
}
func retryWindow(retry int) (time.Duration, time.Duration) {
	ceiling := retryBaseDelay
	for i := 1; i < retry && ceiling < retryMaxDelay; i++ {
		ceiling *= 2
		if ceiling > retryMaxDelay {
			ceiling = retryMaxDelay
		}
	}
	return ceiling / 2, ceiling
}

// runUpload 将重试预算与业务请求分开，等待期间可以响应服务端跳过标记。
// maxRetries 是首次尝试以外的次数；终止后不再调用 attempt。
func (u *Uploader) runUpload(folderName string, report Reporter, progress ProgressFn,
	attempt func(int) (bool, string, string), wait func(time.Duration) bool) (bool, string) {
	if progress != nil {
		progress("starting", -1, -1, 0)
	}
	initial := randomDuration(time.Second, initialMaxDelay)
	fmt.Printf("  [上传] 错峰等待 %.1f 秒后开始\n", initial.Seconds())
	if wait(initial) {
		return true, "服务端已标记，停止本地上传"
	}
	for retry := 0; retry <= u.cfg.MaxRetries; retry++ {
		ok, msg, step := attempt(retry)
		if ok {
			return true, msg
		}
		if retry == u.cfg.MaxRetries {
			final := fmt.Sprintf("重试已达上限：首次尝试 + %d 次重试（共 %d 次），失败步骤：%s，原因：%s；已停止自动重试，保留本地数据", u.cfg.MaxRetries, u.cfg.MaxRetries+1, step, msg)
			if progress != nil {
				progress("retry_exhausted", -1, -1, retry)
			}
			if report != nil {
				report(folderName, "retry_exhausted", final)
			}
			u.logf("✗ %s | 文件夹: %s", final, folderName)
			return false, final
		}
		next := retry + 1
		low, high := retryWindow(next)
		delay := randomDuration(low, high)
		message := fmt.Sprintf("第 %d/%d 次重试将在 %.1f 秒后开始，失败步骤：%s，原因：%s", next, u.cfg.MaxRetries, delay.Seconds(), step, msg)
		fmt.Printf("  [上传] %s\n", message)
		if progress != nil {
			progress("retrying", -1, -1, retry)
		}
		if report != nil {
			report(folderName, "retrying", message)
		}
		if wait(delay) {
			return true, fmt.Sprintf("服务端已标记，停止本地上传重试（上次失败：%s）", msg)
		}
	}
	panic("unreachable retry loop")
}
