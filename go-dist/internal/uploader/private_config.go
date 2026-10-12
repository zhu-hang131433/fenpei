package uploader

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// 平台凭据仅从本机配置读取，不写入源码、安装包和 GitHub Release。
func privateSetting(env, key string) (string, error) {
	if value := os.Getenv(env); value != "" {
		return value, nil
	}
	root, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("无法读取本机配置目录")
	}
	path := filepath.Join(root, "fenpei", "platform.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("请配置 %s 或本机配置文件 %s", env, path)
	}
	var config map[string]string
	if err := json.Unmarshal(data, &config); err != nil {
		return "", fmt.Errorf("本机平台配置格式无效：%s", path)
	}
	if config[key] == "" {
		return "", fmt.Errorf("本机平台配置缺少 %s", key)
	}
	return config[key], nil
}
