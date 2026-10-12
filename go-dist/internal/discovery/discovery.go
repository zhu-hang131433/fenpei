// Package discovery 使用子网 UDP 广播寻找 FenPei 服务，不扫描网段内的其他服务。
package discovery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"
)

const Service = "fenpei-pool-v1"

type message struct {
	Service string `json:"service"`
	Nonce   string `json:"nonce"`
	Port    int    `json:"port,omitempty"`
}

// Listen 与 HTTP 使用同一端口号，但使用 UDP，返回关闭函数。
func Listen(host string, port int) (func(), error) {
	addr, err := net.ResolveUDPAddr("udp4", net.JoinHostPort(host, fmt.Sprint(port)))
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		b := make([]byte, 1024)
		for {
			n, peer, err := conn.ReadFromUDP(b)
			if err != nil {
				return
			}
			var req message
			if json.Unmarshal(b[:n], &req) != nil || req.Service != Service || len(req.Nonce) != 32 || req.Port != 0 {
				continue
			}
			data, _ := json.Marshal(message{Service: Service, Nonce: req.Nonce, Port: port})
			conn.WriteToUDP(data, peer)
		}
	}()
	return func() { conn.Close() }, nil
}

func targets(port int) []*net.UDPAddr {
	out := []*net.UDPAddr{{IP: net.IPv4(127, 0, 0, 1), Port: port}}
	seen := map[string]bool{}
	interfaces, _ := net.Interfaces()
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagBroadcast == 0 {
			continue
		}
		name := strings.ToLower(iface.Name)
		virtual := false
		for _, prefix := range []string{"docker", "veth", "virbr", "br-", "tun", "tap", "wg", "tailscale", "meta", "mihomo", "clash"} {
			if strings.HasPrefix(name, prefix) {
				virtual = true
			}
		}
		if virtual {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipnet.IP.To4()
			if ip == nil || !ip.IsPrivate() {
				continue
			}
			broadcast := make(net.IP, 4)
			mask := ipnet.Mask
			if len(mask) != 4 {
				continue
			}
			for i := 0; i < 4; i++ {
				broadcast[i] = ip[i] | ^mask[i]
			}
			if !seen[broadcast.String()] {
				out = append(out, &net.UDPAddr{IP: broadcast, Port: port})
				seen[broadcast.String()] = true
			}
		}
	}
	return out
}

// Probe 只接受提供明确服务标识的 HTTP 服务；局域网探测不经过系统代理。
func Probe(ctx context.Context, base string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(base, "/")+"/api/discovery", nil)
	if err != nil {
		return err
	}
	hc := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer hc.CloseIdleConnections()
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var result message
	if resp.StatusCode != 200 || json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 4096)).Decode(&result) != nil || result.Service != Service {
		return fmt.Errorf("该地址不是 FenPei 服务端")
	}
	return nil
}

func Find(ctx context.Context, port int, duration time.Duration) (string, error) {
	return find(ctx, targets(port), duration)
}
func find(ctx context.Context, addresses []*net.UDPAddr, duration time.Duration) (string, error) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		return "", err
	}
	defer conn.Close()
	nonceBytes := make([]byte, 16)
	if _, err = rand.Read(nonceBytes); err != nil {
		return "", err
	}
	nonce := hex.EncodeToString(nonceBytes)
	data, _ := json.Marshal(message{Service: Service, Nonce: nonce})
	for _, target := range addresses {
		conn.WriteToUDP(data, target)
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	conn.SetReadDeadline(time.Now().Add(duration))
	b := make([]byte, 1024)
	found := map[string]bool{}
	for {
		n, peer, err := conn.ReadFromUDP(b)
		if err != nil {
			break
		}
		var response message
		if json.Unmarshal(b[:n], &response) == nil && response.Service == Service && response.Nonce == nonce && response.Port > 0 && response.Port <= 65535 {
			found["http://"+net.JoinHostPort(peer.IP.String(), fmt.Sprint(response.Port))] = true
		}
	}
	list := make([]string, 0, len(found))
	for base := range found {
		list = append(list, base)
	}
	sort.Strings(list)
	for _, base := range list {
		if Probe(ctx, base) == nil {
			return base, nil
		}
	}
	return "", fmt.Errorf("尚未发现同一局域网内的服务端，正在重试")
}
