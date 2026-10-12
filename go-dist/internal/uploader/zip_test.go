package uploader

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLargeStreamCompressionReportsEachByteOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.bin")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	const length = 33 << 20
	if err := file.Truncate(length); err != nil {
		t.Fatal(err)
	}
	file.Close()
	var output bytes.Buffer
	writer := zip.NewWriter(&output)
	var reported int64
	if err := streamEntry(writer, zipEntry{rel: "large.bin", abs: path, size: length}, func(n int64) { reported += n }); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if reported != length {
		t.Fatalf("reported %d bytes, expected %d", reported, length)
	}
	reader, err := zip.NewReader(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.File) != 1 || reader.File[0].UncompressedSize64 != length {
		t.Fatal("ZIP entry size differs from real progress")
	}
}
