package main

import (
	"flag"
	"io"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

func main() {
	hubURL := flag.String("hub", "ws://127.0.0.1:18080/ws", "http server websocket url")
	proxyKey := flag.String("proxy-key", "", "proxy key (reusable while gateway key file stays unchanged)")
	listenAddr := flag.String("listen", "127.0.0.1:14096", "local tcp listen address for tui/opencode client")
	flag.Parse()

	if strings.TrimSpace(*proxyKey) == "" {
		log.Fatalf("-proxy-key is required")
	}

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	log.Printf("client proxy listening on tcp://%s", *listenAddr)
	log.Printf("waiting for tcp client connections...")

	wsURL, err := buildClientURL(*hubURL, *proxyKey)
	if err != nil {
		log.Fatalf("invalid hub url: %v", err)
	}

	for {
		tcpConn, err := ln.Accept()
		if err != nil {
			log.Printf("accept failed: %v", err)
			continue
		}

		log.Printf("tcp client connected, dialing websocket hub...")
		wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			_ = tcpConn.Close()
			log.Printf("dial websocket failed: %v", err)
			continue
		}

		log.Printf("websocket paired with hub")
		log.Printf("websocket connected, start forwarding traffic")
		go func(tc net.Conn, ws *websocket.Conn) {
			defer tc.Close()
			defer ws.Close()
			bridgeTCPWS(tc, ws)
			log.Printf("proxy session ended")
		}(tcpConn, wsConn)
	}
}

func buildClientURL(rawHubURL, proxyKey string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(rawHubURL))
	if err != nil {
		return "", err
	}
	if u.Scheme == "http" {
		u.Scheme = "ws"
	} else if u.Scheme == "https" {
		u.Scheme = "wss"
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ws"
	}
	q := u.Query()
	q.Set("role", "client")
	q.Set("proxy_key", proxyKey)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func bridgeTCPWS(tcpConn net.Conn, wsConn *websocket.Conn) {
	errCh := make(chan error, 2)
	var writeMu sync.Mutex

	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := tcpConn.Read(buf)
			if err != nil {
				errCh <- err
				return
			}
			if n == 0 {
				continue
			}
			writeMu.Lock()
			werr := wsConn.WriteMessage(websocket.BinaryMessage, buf[:n])
			writeMu.Unlock()
			if werr != nil {
				errCh <- werr
				return
			}
		}
	}()

	go func() {
		for {
			messageType, payload, err := wsConn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			if messageType != websocket.BinaryMessage && messageType != websocket.TextMessage {
				continue
			}
			if len(payload) == 0 {
				continue
			}
			if _, err := tcpConn.Write(payload); err != nil {
				errCh <- err
				return
			}
		}
	}()

	err := <-errCh
	if err != nil && err != io.EOF {
		log.Printf("bridge closed: %v", err)
	}
}
