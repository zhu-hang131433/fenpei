// Package application 提供同一程序的登录入口，登录后启动服务端或客户端。
package application

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"fenpei/internal/client"
	"fenpei/internal/discovery"
	"fenpei/internal/server"
)

func Run(ctx context.Context, opt client.AgentOptions) error {
	if opt.LocalPort == 0 {
		opt.LocalPort = 18080
	}
	if opt.DiscoveryPort == 0 {
		opt.DiscoveryPort = 8000
	}
	address := fmt.Sprintf("127.0.0.1:%d", opt.LocalPort)
	local, err := net.Listen("tcp4", address)
	if err != nil {
		return fmt.Errorf("应用端口不可用（请检查是否已经启动）: %w", err)
	}
	defer local.Close()
	pool := server.New("")
	var mu sync.Mutex
	var agent *client.Agent
	var agentHandler http.Handler
	var lan *http.Server
	var stopDiscovery func()
	activate := func(role string) error {
		mu.Lock()
		defer mu.Unlock()
		if role == "admin" && lan == nil {
			listener, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", opt.DiscoveryPort))
			if err != nil {
				return fmt.Errorf("服务端端口 %d 被占用，请先退出旧服务端: %w", opt.DiscoveryPort, err)
			}
			closeDiscovery, err := discovery.Listen("0.0.0.0", opt.DiscoveryPort)
			if err != nil {
				listener.Close()
				return fmt.Errorf("服务端自动发现端口不可用: %w", err)
			}
			pool.Start()
			stopDiscovery = closeDiscovery
			lan = &http.Server{Handler: pool.Handler(), ReadHeaderTimeout: 10 * time.Second}
			go lan.Serve(listener)
			fmt.Printf("服务端已启动，局域网端口: %d\n", opt.DiscoveryPort)
		}
		if role == "operator" && agent == nil {
			newAgent, err := client.NewAgent(opt)
			if err != nil {
				return err
			}
			agent = newAgent
			agentHandler = agent.Handler(opt.LocalPort)
			go agent.Connect(ctx)
		}
		return nil
	}
	localAPI := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		h := agentHandler
		mu.Unlock()
		if h == nil {
			http.Error(w, "客户端尚未启动，请重新登录", 503)
			return
		}
		h.ServeHTTP(w, r)
	})
	handler := pool.ApplicationHandler(activate, localAPI)
	// 本机入口只接受回环 Host，防止 DNS 重绑定。
	guarded := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != address && r.Host != fmt.Sprintf("localhost:%d", opt.LocalPort) {
			http.Error(w, "invalid host", 403)
			return
		}
		handler.ServeHTTP(w, r)
	})
	ui := &http.Server{Handler: guarded, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); ui.Close() }()
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		if agent != nil {
			agent.Close()
		}
		if lan != nil {
			lan.Close()
			stopDiscovery()
			pool.Stop()
		}
	}()
	pageURL := "http://" + address + "/login"
	fmt.Println("FenPei 登录入口:", pageURL)
	if opt.OpenBrowser {
		go func() {
			if err := exec.Command("xdg-open", pageURL).Run(); err != nil {
				fmt.Println("请在浏览器打开:", pageURL)
			}
		}()
	}
	err = ui.Serve(local)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
