package client

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fenpei/internal/protocol"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDownloadProgressCountsResumedAndParallelBytesOnce(t *testing.T) {
	small := bytes.Repeat([]byte("small"), 10000)
	big := bytes.Repeat([]byte("parallel"), 1024*1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data := small
		if r.URL.Path == "/files/task/big.bin" {
			data = big
		}
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(data))
	}))
	defer server.Close()
	root := t.TempDir()
	os.Mkdir(filepath.Join(root, "task"), 0755)
	if err := os.WriteFile(filepath.Join(root, "task", "small.bin"), small[:1000], 0644); err != nil {
		t.Fatal(err)
	}
	c := &Client{opt: Options{Server: server.URL, Dest: root, Files: 2, Threads: 4, Chunk: 1 << 20}}
	if !c.downloadAll([]protocol.FileInfo{{Path: "task/small.bin", Size: int64(len(small))}, {Path: "task/big.bin", Size: int64(len(big))}}) {
		t.Fatal("download failed")
	}
	if got := c.doneBytes.Load(); got != int64(len(small)+len(big)) {
		t.Fatalf("wrong byte progress: %d", got)
	}
	for name, want := range map[string][]byte{"small.bin": small, "big.bin": big} {
		got, err := os.ReadFile(filepath.Join(root, "task", name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("file %s corrupted: %v", name, err)
		}
	}
	c.doneBytes.Store(0)
	if err := c.downloadFile(protocol.FileInfo{Path: "task/small.bin", Size: int64(len(small))}, filepath.Join(root, "task", "small.bin")); err != nil {
		t.Fatal(err)
	}
	if c.doneBytes.Load() != int64(len(small)) {
		t.Fatal("already-downloaded file omitted from progress")
	}
}

func TestDownloadRejectsSameSizeStaleFileAndCorruptSource(t *testing.T) {
	fresh := []byte("fresh data")
	sum := sha256.Sum256(fresh)
	fi := protocol.FileInfo{Path: "task/data", Size: int64(len(fresh)), SHA256: hex.EncodeToString(sum[:])}
	response := fresh
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(response))
	}))
	defer server.Close()
	dest := filepath.Join(t.TempDir(), "data")
	os.WriteFile(dest, []byte("stale data"), 0644)
	c := &Client{opt: Options{Server: server.URL}}
	if err := c.downloadFile(fi, dest); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(dest)
	if !bytes.Equal(got, fresh) {
		t.Fatal("stale content reused")
	}
	first := requests
	c.doneBytes.Store(0)
	if err := c.downloadFile(fi, dest); err != nil {
		t.Fatal(err)
	}
	if requests != first {
		t.Fatal("verified file downloaded again")
	}
	os.WriteFile(dest, []byte("old"), 0644)
	response = []byte("wrong data")
	c.doneBytes.Store(0)
	if err := c.downloadFile(fi, dest); err == nil {
		t.Fatal("bad checksum accepted")
	}
	if c.doneBytes.Load() != 0 {
		t.Fatal("failed download counted")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("corrupt file retained")
	}
}

func TestOldExtrasQuarantinedOutsideTask(t *testing.T) {
	root := t.TempDir()
	taskDir := filepath.Join(root, "source", "task")
	os.MkdirAll(taskDir, 0755)
	os.WriteFile(filepath.Join(taskDir, "keep"), []byte("keep"), 0644)
	os.WriteFile(filepath.Join(taskDir, "old"), []byte("old"), 0644)
	c := &Client{opt: Options{Dest: root}}
	task := &protocol.NextResp{Folder: "source/task", Files: []protocol.FileInfo{{Path: "source/task/keep"}}}
	if err := c.quarantineExtraFiles(task); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(taskDir, "old")); !os.IsNotExist(err) {
		t.Fatal("extra file remains")
	}
	backups, _ := filepath.Glob(filepath.Join(root, ".stale-task-*", "old"))
	if len(backups) != 1 {
		t.Fatal("old file not preserved")
	}
	if got, _ := os.ReadFile(backups[0]); string(got) != "old" {
		t.Fatal("backup corrupted")
	}
	if _, err := os.Stat(filepath.Join(taskDir, "keep")); err != nil {
		t.Fatal(err)
	}
}
