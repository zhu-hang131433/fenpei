package uploader

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Minio 最小化 S3 客户端（PutObject/EnsureBucket，AWS Signature V4，
// UNSIGNED-PAYLOAD 流式上传），行为与 Python minio-go 版一致（secure=False 走 http）。
type Minio struct {
	endpoint string // host:port
	ak, sk   string
	hc       *http.Client
}

func NewMinio(endpoint, ak, sk string) *Minio {
	return &Minio{
		endpoint: endpoint,
		ak:       ak,
		sk:       sk,
		hc: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:          8,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 120 * time.Second,
			},
		},
	}
}

// s3URLEncode 按 S3 规则编码（保留 '/'）
func s3URLEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '.' || c == '_' || c == '~' || c == '/' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func (m *Minio) sign(req *http.Request, payloadHash string) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	region := "us-east-1"

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	host := req.URL.Host

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		"", // query
		"host:" + host + "\nx-amz-content-sha256:" + payloadHash + "\nx-amz-date:" + amzDate + "\n",
		"host;x-amz-content-sha256;x-amz-date",
		payloadHash,
	}, "\n")

	h := sha256.Sum256([]byte(canonicalRequest))
	canonicalHash := hex.EncodeToString(h[:])

	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		dateStamp + "/" + region + "/s3/aws4_request",
		canonicalHash,
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+m.sk), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	sig := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s/%s/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=%s",
		m.ak, dateStamp, region, sig))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func (m *Minio) do(method, bucket, object string, body io.Reader, size int64) (*http.Response, error) {
	u := "http://" + m.endpoint + "/" + s3URLEncode(bucket)
	if object != "" {
		u += "/" + s3URLEncode(object)
	}
	req, err := http.NewRequest(method, u, body)
	if err != nil {
		return nil, err
	}
	if size >= 0 {
		req.ContentLength = size
	}
	m.sign(req, "UNSIGNED-PAYLOAD")
	return m.hc.Do(req)
}

// EnsureBucket 确保 bucket 存在（不存在则创建）
func (m *Minio) EnsureBucket(bucket string) error {
	resp, err := m.do("HEAD", bucket, "", nil, -1)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode == 200 {
		return nil
	}
	if resp.StatusCode != 404 {
		return fmt.Errorf("bucket 检查失败: HTTP %d", resp.StatusCode)
	}
	resp2, err := m.do("PUT", bucket, "", nil, 0)
	if err != nil {
		return err
	}
	defer resp2.Body.Close()
	if resp2.StatusCode/100 != 2 {
		data, _ := io.ReadAll(resp2.Body)
		return fmt.Errorf("创建 bucket 失败: HTTP %d %s", resp2.StatusCode, truncStr(string(data), 200))
	}
	return nil
}

// PutObjectFile 上传文件到指定对象（带上传进度回调，0-100）
func (m *Minio) PutObjectFile(bucket, object, path string, progress func(int)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	pr := &progressReader{r: f, total: fi.Size(), cb: progress}
	resp, err := m.do("PUT", bucket, object, pr, fi.Size())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d %s", resp.StatusCode, truncStr(string(data), 200))
	}
	io.Copy(io.Discard, resp.Body)
	if progress != nil {
		progress(100)
	}
	return nil
}

type progressReader struct {
	r        io.Reader
	total    int64
	done     int64
	lastTime time.Time
	lastPct  int
	cb       func(int)
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.done += int64(n)
		if p.cb != nil && p.total > 0 && time.Since(p.lastTime) >= 500*time.Millisecond {
			pct := int(p.done * 100 / p.total)
			if pct != p.lastPct {
				p.lastTime = time.Now()
				p.lastPct = pct
				p.cb(pct)
			}
		}
	}
	return n, err
}
