package uploader

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

const (
	// 并行压缩的内存上限策略：小文件（≤32MB）由 worker 池并行压缩后按序写入，
	// 大文件走流式压缩，避免整文件进内存。
	zipParallelThreshold = int64(32 << 20)
)

type zipEntry struct {
	rel, abs string
	size     int64
	mod      time.Time
}

type zipResult struct {
	buf   *bytes.Buffer
	crc   uint32
	csize uint64
	err   error
}

// ZipFolder 将文件夹压缩为 zip（zip 名 = 文件夹名.zip，与平台 GUI 一致）。
// 并行压缩 CPU 密集的 deflate，进度按原始字节推进（0-100）。
func ZipFolder(src, dst string, progress func(int)) error {
	var ents []zipEntry
	var total int64
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return nil
		}
		ents = append(ents, zipEntry{rel: filepath.ToSlash(rel), abs: p, size: info.Size(), mod: info.ModTime()})
		total += info.Size()
		return nil
	})
	if err != nil {
		return err
	}

	tmp := dst + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	zw := zip.NewWriter(f)

	workers := runtime.NumCPU()
	if workers > 4 {
		workers = 4
	}
	if workers < 2 {
		workers = 1
	}

	queued := map[int]bool{}
	tasks := make(chan int, workers)
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	results := map[int]*zipResult{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range tasks {
				r := compressToBuffer(ents[idx].abs)
				mu.Lock()
				results[idx] = r
				mu.Unlock()
				cond.Broadcast()
			}
		}()
	}
	for i, e := range ents {
		if e.size > 0 && e.size <= zipParallelThreshold {
			queued[i] = true
			tasks <- i
		}
	}

	var written int64
	lastReport := time.Now()
	report := func(n int64) {
		written += n
		if progress != nil && total > 0 && time.Since(lastReport) >= 300*time.Millisecond {
			lastReport = time.Now()
			pct := int(written * 100 / total)
			if pct > 100 {
				pct = 100
			}
			progress(pct)
		}
	}

	var zerr error
	for i, e := range ents {
		if queued[i] {
			mu.Lock()
			for {
				r, ok := results[i]
				if ok {
					delete(results, i)
					mu.Unlock()
					if r.err != nil {
						zerr = r.err
					} else if err := writeRawEntry(zw, e, r); err != nil {
						zerr = err
					} else {
						report(e.size)
					}
					break
				}
				cond.Wait()
			}
		} else {
			if err := streamEntry(zw, e, report); err != nil {
				zerr = err
			}
		}
		if zerr != nil {
			break
		}
	}
	close(tasks)
	wg.Wait()
	if zerr == nil {
		if err := zw.Close(); err != nil {
			zerr = err
		}
	}
	if cerr := f.Close(); zerr == nil {
		zerr = cerr
	}
	if zerr != nil {
		os.Remove(tmp)
		return zerr
	}
	if progress != nil {
		progress(100)
	}
	return os.Rename(tmp, dst)
}

func compressToBuffer(path string) *zipResult {
	f, err := os.Open(path)
	if err != nil {
		return &zipResult{err: err}
	}
	defer f.Close()
	var buf bytes.Buffer
	dw, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return &zipResult{err: err}
	}
	h := crc32.NewIEEE()
	if _, err := io.Copy(io.MultiWriter(dw, h), f); err != nil {
		return &zipResult{err: err}
	}
	if err := dw.Close(); err != nil {
		return &zipResult{err: err}
	}
	return &zipResult{buf: &buf, crc: h.Sum32(), csize: uint64(buf.Len())}
}

func writeRawEntry(zw *zip.Writer, e zipEntry, r *zipResult) error {
	h := &zip.FileHeader{Name: e.rel, Method: zip.Deflate, Modified: e.mod}
	h.CRC32 = r.crc
	h.CompressedSize64 = r.csize
	h.UncompressedSize64 = uint64(e.size)
	w, err := zw.CreateRaw(h)
	if err != nil {
		return err
	}
	_, err = w.Write(r.buf.Bytes())
	return err
}

func streamEntry(zw *zip.Writer, e zipEntry, report func(int64)) error {
	h := &zip.FileHeader{Name: e.rel, Method: zip.Deflate, Modified: e.mod}
	w, err := zw.CreateHeader(h)
	if err != nil {
		return err
	}
	f, err := os.Open(e.abs)
	if err != nil {
		return err
	}
	defer f.Close()
	cw := &countingWriter{w: w, cb: report}
	_, err = io.Copy(cw, f)
	return err
}

type countingWriter struct {
	w  io.Writer
	cb func(int64)
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	if n > 0 && cw.cb != nil {
		cw.cb(int64(n))
	}
	return n, err
}
