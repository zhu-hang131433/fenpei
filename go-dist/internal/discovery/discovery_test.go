package discovery

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestFindRequiresUDPAndHTTPIdentity(t *testing.T) {
	valid := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !valid {
			http.Error(w, "other service", 404)
			return
		}
		json.NewEncoder(w).Encode(message{Service: Service})
	}))
	defer server.Close()
	addr, err := net.ResolveUDPAddr("udp4", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	closeDiscovery, err := Listen("127.0.0.1", addr.Port)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDiscovery()
	found, err := find(context.Background(), []*net.UDPAddr{addr}, 50*time.Millisecond)
	if err != nil || found != server.URL {
		t.Fatalf("wanted %s, got %s %v", server.URL, found, err)
	}
	valid = false
	if _, err = find(context.Background(), []*net.UDPAddr{addr}, 50*time.Millisecond); err == nil {
		t.Fatal("non-FenPei HTTP service accepted")
	}
}
func TestFindRejectsWrongNonce(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	go func() {
		b := make([]byte, 1024)
		_, peer, err := conn.ReadFromUDP(b)
		if err == nil {
			data, _ := json.Marshal(message{Service: Service, Nonce: "wrong", Port: 8000})
			conn.WriteToUDP(data, peer)
		}
	}()
	if _, err := find(context.Background(), []*net.UDPAddr{conn.LocalAddr().(*net.UDPAddr)}, 50*time.Millisecond); err == nil {
		t.Fatal("mismatched discovery nonce accepted")
	}
}
func TestProbeRejectsRedirectAndCancelStopsSearch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/login", 303) }))
	defer server.Close()
	if Probe(context.Background(), server.URL) == nil {
		t.Fatal("redirect accepted as service identity")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, err := find(ctx, []*net.UDPAddr{{IP: net.IPv4(127, 0, 0, 1), Port: 12345}}, time.Second)
	if err == nil || time.Since(start) > 500*time.Millisecond {
		t.Fatal("canceled discovery did not stop")
	}
	// 所有广播目的地使用指定发现端口。
	for _, addr := range targets(12345) {
		if strconv.Itoa(addr.Port) != "12345" {
			t.Fatal("wrong discovery port")
		}
	}
}
