// fenpei-client 下载后运行，自动发现服务端并打开独立客户端页面。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"fenpei/internal/application"
	"fenpei/internal/client"
	"fenpei/internal/uploader"
)

func main() {
	server := flag.String("server", "", "可选：指定服务端地址；默认自动发现局域网服务端")
	dest := flag.String("dest", "./received", "文件保存目录")
	threads := flag.Int("threads", 4, "单文件分块并行下载线程数")
	files := flag.Int("files", 2, "同时下载的文件数")
	chunk := flag.Int64("chunk", 4*1024*1024, "分块大小（字节）")
	retries := flag.Int("upload-retries", uploader.DefaultMaxRetries, "上传最大重试次数，不含首次尝试（1～20）")
	upload := flag.Bool("upload", true, "下载后自动上传；--upload=false 仅下载")
	headless := flag.Bool("headless", false, "命令行模式，连接后直接工作")
	worker := flag.Bool("worker", false, "内部工作进程（由客户端页面控制器启动）")
	localPort := flag.Int("ui-port", 18080, "本机客户端页面端口")
	discoverPort := flag.Int("discovery-port", 8000, "服务端自动发现 UDP 端口")
	noBrowser := flag.Bool("no-browser", false, "不自动打开浏览器")
	flag.Parse()
	if *retries < 1 || *retries > uploader.MaximumMaxRetries {
		fmt.Println("错误：--upload-retries 必须在 1～20 之间")
		os.Exit(1)
	}
	if *localPort < 1 || *localPort > 65535 || *discoverPort < 1 || *discoverPort > 65535 {
		fmt.Println("错误：端口必须在 1～65535 之间")
		os.Exit(1)
	}
	if *server != "" && !client.ValidServer(*server) {
		fmt.Println("错误：服务端地址必须是有效的 HTTP/HTTPS 地址")
		os.Exit(1)
	}
	opt := client.Options{Server: *server, Dest: *dest, Threads: *threads, Files: *files, Chunk: *chunk, Upload: *upload, UploadRetries: *retries}
	var err error
	if *worker || *headless {
		if *server == "" {
			fmt.Println("错误：命令行模式必须指定 --server")
			os.Exit(1)
		}
		if *worker {
			opt.Observe = func(state client.WorkState) {
				data, _ := json.Marshal(state)
				fmt.Printf("\n%s%s\n", client.StateMarker, data)
			}
		}
		err = client.Run(opt)
	} else {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		err = application.Run(ctx, client.AgentOptions{Options: opt, LocalPort: *localPort, DiscoveryPort: *discoverPort, OpenBrowser: !*noBrowser})
	}
	if err != nil {
		fmt.Println("错误:", err)
		os.Exit(1)
	}
}
