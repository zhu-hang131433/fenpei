// fenpei-server 文件分发服务器（源端，只读，按文件夹分发）
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"fenpei/internal/discovery"
	"fenpei/internal/server"
)

func main() {
	var (
		source       = flag.String("source", "", "可选：启动时加入池子的目录，也可在页面动态添加")
		port         = flag.Int("port", 8000, "监听端口")
		host         = flag.String("host", "0.0.0.0", "监听地址")
		selfClient   = flag.Bool("self-client", false, "在本机也启动客户端（自发自收）")
		clientUpload = flag.Bool("client-upload", false, "本机客户端下载后自动上传到吉翼数采")
	)
	flag.Bool("resume", false, "兼容参数；默认恢复工作目录中的任务池")
	flag.Parse()

	abs := ""
	if *source != "" {
		var err error
		abs, err = filepath.Abs(*source)
		if err != nil {
			fmt.Println("错误：", err)
			os.Exit(1)
		}
		if fi, err := os.Stat(abs); err != nil || !fi.IsDir() {
			fmt.Println("错误：目录不存在:", abs)
			os.Exit(1)
		}
	}

	s := server.New(abs)
	s.Start()
	defer s.Stop()
	if closeDiscovery, err := discovery.Listen(*host, *port); err != nil {
		log.Printf("局域网自动发现暂不可用: %v", err)
	} else {
		defer closeDiscovery()
	}

	addr := net.JoinHostPort(*host, fmt.Sprint(*port))
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("文件分发服务器启动（按文件夹分发）[Go 版]")
	fmt.Println("动态任务池：在页面的“数据池”中展开“加入硬盘数据”")
	fmt.Printf("监听地址: %s\n", addr)
	fmt.Println(strings.Repeat("=", 60))

	if *host == "0.0.0.0" || *host == "::" || *host == "" {
		fmt.Printf("\n本机浏览器打开: http://127.0.0.1:%d\n", *port)
		ips := detectLocalIPs()
		if len(ips) == 0 {
			fmt.Println("未找到可用的局域网 IPv4 地址，请检查网卡连接。")
		}
		for _, ip := range ips {
			printClientAddress(ip, *port)
		}
	} else {
		fmt.Printf("\n浏览器打开: http://%s\n", addr)
		if ip := net.ParseIP(*host); *host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			printClientAddress(*host, *port)
		} else {
			fmt.Println("当前只监听本机地址，其他电脑无法连接。")
		}
	}
	fmt.Println()

	var selfProc *exec.Cmd
	if *selfClient {
		selfProc = startSelfClient(abs, *port, *clientUpload)
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("收到退出信号，保存状态并退出...")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		httpSrv.Shutdown(ctx)
	}()

	err := httpSrv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	if selfProc != nil && selfProc.Process != nil {
		selfProc.Process.Kill()
	}
}

// startSelfClient 启动本机客户端（fenpei-client 与服务端二进制同目录，可用 FENPEI_CLIENT 覆盖）
func startSelfClient(source string, port int, upload bool) *exec.Cmd {
	bin := os.Getenv("FENPEI_CLIENT")
	if bin == "" {
		exe, err := os.Executable()
		if err == nil {
			cand := filepath.Join(filepath.Dir(exe), "fenpei-client")
			if _, err := os.Stat(cand); err == nil {
				bin = cand
			}
		}
	}
	if bin == "" {
		bin = "./fenpei-client"
	}
	dest := "./received"
	if source != "" {
		dest = filepath.Join(filepath.Dir(source), "received")
	}
	args := []string{"--headless", "--server", fmt.Sprintf("http://127.0.0.1:%d", port), "--dest", dest, "--upload=" + fmt.Sprint(upload)}
	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	fmt.Printf("启动本机客户端: %s %s\n", bin, strings.Join(args, " "))
	if err := cmd.Start(); err != nil {
		log.Printf("启动本机客户端失败: %v", err)
		return nil
	}
	return cmd
}

// detectLocalIPs 枚举实际联网网卡，不通过出口路由探测，避免选中代理 TUN。
// 多个有效地址全部列出，由用户按客户端所在网络选择。
func detectLocalIPs() []string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var private, other []string
	seen := make(map[string]bool)
	_, benchmark, _ := net.ParseCIDR("198.18.0.0/15")
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagPointToPoint != 0 {
			continue
		}
		name := strings.ToLower(iface.Name)
		virtual := false
		for _, prefix := range []string{"meta", "mihomo", "clash", "tun", "tap", "utun", "docker", "veth", "virbr", "br-", "wg", "tailscale", "zt"} {
			if strings.HasPrefix(name, prefix) {
				virtual = true
				break
			}
		}
		if virtual {
			continue
		}
		addresses, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err != nil || ip.To4() == nil || !ip.IsGlobalUnicast() || benchmark.Contains(ip) {
				continue
			}
			value := ip.String()
			if seen[value] {
				continue
			}
			seen[value] = true
			if ip.IsPrivate() {
				private = append(private, value)
			} else {
				other = append(other, value)
			}
		}
	}
	sort.Strings(private)
	sort.Strings(other)
	return append(private, other...)
}

func printClientAddress(host string, port int) {
	address := "http://" + net.JoinHostPort(host, fmt.Sprint(port))
	fmt.Printf("\n其他电脑访问（选择同一网络的地址）: %s\n", address)
	fmt.Println("客户端电脑执行：")
	fmt.Printf("  wget %s/client -O fenpei-client && chmod +x fenpei-client && ./fenpei-client --server %s --dest ./received\n", address, address)
}
