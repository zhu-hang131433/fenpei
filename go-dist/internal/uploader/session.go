package uploader

// uploadSession 仅在当前文件夹处理期间保存已确认成功的步骤。
// 每个客户端自己的 Uploader 独立维护，不限制其他客户端同时上传。
type uploadSession struct {
	FolderPath       string
	FolderName       string
	Platform         *Platform
	TaskID           string
	TaskItem         map[string]any
	RefreshLifecycle bool
	HealthDone       bool
	StartDone        bool
	IdleSent         bool
	CollectionDone   bool
	Info             map[string]any
	DAG              map[string]any
	BucketChecked    bool
	ObjectUploaded   bool
}
