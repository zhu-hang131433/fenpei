package uploader

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateConfigAndEnvironmentOverride(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("FENPEI_PLATFORM_PASSWORD", "")
	if _, err := privateSetting("FENPEI_PLATFORM_PASSWORD", "platform_password"); err == nil {
		t.Fatal("missing configuration accepted")
	}
	os.Mkdir(filepath.Join(root, "fenpei"), 0700)
	os.WriteFile(filepath.Join(root, "fenpei", "platform.json"), []byte(`{"platform_password":"local-only"}`), 0600)
	if value, err := privateSetting("FENPEI_PLATFORM_PASSWORD", "platform_password"); err != nil || value != "local-only" {
		t.Fatal("local config failed", err)
	}
	t.Setenv("FENPEI_PLATFORM_PASSWORD", "environment-only")
	if value, err := privateSetting("FENPEI_PLATFORM_PASSWORD", "platform_password"); err != nil || value != "environment-only" {
		t.Fatal("override failed", err)
	}
}
