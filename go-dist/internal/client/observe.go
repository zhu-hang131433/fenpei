package client

import (
	"fenpei/internal/protocol"
)

const StateMarker = "@fenpei-state:"

// WorkState 来自下载/上传引擎的真实回调，不由页面猜测进度。
type WorkState struct {
	WorkerID        string                     `json:"worker_id"`
	Label           string                     `json:"label"`
	Progress        protocol.FolderProgressReq `json:"progress"`
	DownloadedBytes int64                      `json:"downloaded_bytes"`
	TotalBytes      int64                      `json:"total_bytes"`
	Completed       int64                      `json:"completed"`
	Failed          int64                      `json:"failed"`
}

func (c *Client) emit() {
	if c.opt.Observe == nil {
		return
	}
	c.pmu.Lock()
	progress := c.prog
	c.pmu.Unlock()
	c.opt.Observe(WorkState{WorkerID: c.workerID, Label: c.label, Progress: progress, DownloadedBytes: c.doneBytes.Load(), TotalBytes: c.totalBytes, Completed: c.completed.Load(), Failed: c.failed.Load()})
}
