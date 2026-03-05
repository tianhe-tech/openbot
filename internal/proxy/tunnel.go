package proxy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type KeyFile struct {
	ProxyKey  string    `json:"proxy_key"`
	HubWSURL  string    `json:"hub_ws_url"`
	CreatedAt time.Time `json:"created_at"`
}

func GenerateProxyKey() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func WriteKeyFile(filePath, hubWSURL, key string) error {
	payload := KeyFile{
		ProxyKey:  key,
		HubWSURL:  hubWSURL,
		CreatedAt: time.Now().UTC(),
	}
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, b, 0o600)
}

func PrepareRuntimeKey(filePath, hubWSURL string) (string, error) {
	key, err := GenerateProxyKey()
	if err != nil {
		return "", err
	}
	if err := WriteKeyFile(filePath, hubWSURL, key); err != nil {
		return "", err
	}
	return key, nil
}

// controlMsg matches the hub's controlMsg structure.
type controlMsg struct {
	Action string `json:"action"`
	Token  string `json:"token"`
}

func StartGatewayTunnel(ctx context.Context, hubWSURL, proxyKey, localOpenCodeAddr string, reconnectDelay time.Duration) {
	if reconnectDelay <= 0 {
		reconnectDelay = 5 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := runControlLoop(ctx, hubWSURL, proxyKey, localOpenCodeAddr); err != nil {
			log.Printf("proxy tunnel: control loop ended: %v", err)
		}
		log.Printf("proxy tunnel: reconnecting in %s", reconnectDelay)
		sleepWithContext(ctx, reconnectDelay)
	}
}

// runControlLoop connects to the hub as the control WS, then listens for
// "connect" commands. For each command it dials a fresh data WS and bridges
// it to the local opencode TCP service.
func runControlLoop(ctx context.Context, hubWSURL, proxyKey, localOpenCodeAddr string) error {
	ctrlURL, err := buildWSURL(hubWSURL, map[string]string{
		"role":      "control",
		"proxy_key": proxyKey,
	})
	if err != nil {
		return fmt.Errorf("build control url: %w", err)
	}

	wsConn, _, err := websocket.DefaultDialer.DialContext(ctx, ctrlURL, nil)
	if err != nil {
		return fmt.Errorf("dial control: %w", err)
	}
	defer wsConn.Close()

	log.Printf("proxy tunnel: control connected hub=%s key=%s", hubWSURL, proxyKey[:min(10, len(proxyKey))])

	for {
		_, msg, err := wsConn.ReadMessage()
		if err != nil {
			return fmt.Errorf("control read: %w", err)
		}

		var cmd controlMsg
		if err := json.Unmarshal(msg, &cmd); err != nil {
			log.Printf("proxy tunnel: invalid control msg: %v", err)
			continue
		}

		if cmd.Action != "connect" || cmd.Token == "" {
			log.Printf("proxy tunnel: unknown control action %q", cmd.Action)
			continue
		}

		log.Printf("proxy tunnel: received connect token=%s", cmd.Token[:min(8, len(cmd.Token))])

		go func(token string) {
			if err := dialDataSession(ctx, hubWSURL, token, localOpenCodeAddr); err != nil {
				log.Printf("proxy tunnel: data session failed: %v", err)
			}
		}(cmd.Token)
	}
}

// dialDataSession dials the hub's data WS for a specific session token,
// then bridges it to the local opencode TCP service.
func dialDataSession(ctx context.Context, hubWSURL, token, localOpenCodeAddr string) error {
	dataURL, err := buildWSURL(hubWSURL, map[string]string{
		"role":  "data",
		"token": token,
	})
	if err != nil {
		return fmt.Errorf("build data url: %w", err)
	}

	wsConn, _, err := websocket.DefaultDialer.DialContext(ctx, dataURL, nil)
	if err != nil {
		return fmt.Errorf("dial data WS: %w", err)
	}

	log.Printf("proxy tunnel: data WS connected token=%s", token[:min(8, len(token))])
	bridgeWSTCP(wsConn, localOpenCodeAddr)
	log.Printf("proxy tunnel: data session ended token=%s", token[:min(8, len(token))])
	return nil
}

func buildWSURL(rawHub string, params map[string]string) (string, error) {
	trimmed := strings.TrimSpace(rawHub)
	if trimmed == "" {
		return "", fmt.Errorf("empty hub url")
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return "", err
	}
	if u.Scheme == "http" {
		u.Scheme = "ws"
	} else if u.Scheme == "https" {
		u.Scheme = "wss"
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return "", fmt.Errorf("unsupported hub url scheme %q", u.Scheme)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ws"
	}
	q := u.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// bridgeWSTCP dials localOpenCodeAddr immediately and then bidirectionally
// proxies between the WebSocket and the TCP connection.
func bridgeWSTCP(wsConn *websocket.Conn, localOpenCodeAddr string) {
	defer wsConn.Close()

	tcpConn, err := net.Dial("tcp", localOpenCodeAddr)
	if err != nil {
		log.Printf("proxy tunnel: dial local tcp %s failed: %v", localOpenCodeAddr, err)
		return
	}
	defer tcpConn.Close()
	log.Printf("proxy tunnel: local tcp connected %s", localOpenCodeAddr)

	errCh := make(chan error, 2)
	var wsMu sync.Mutex // gorilla requires serialised writes

	// TCP → WS
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := tcpConn.Read(buf)
			if n > 0 {
				wsMu.Lock()
				werr := wsConn.WriteMessage(websocket.BinaryMessage, buf[:n])
				wsMu.Unlock()
				if werr != nil {
					errCh <- werr
					return
				}
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	// WS → TCP
	go func() {
		for {
			msgType, payload, err := wsConn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			if (msgType == websocket.BinaryMessage || msgType == websocket.TextMessage) && len(payload) > 0 {
				if _, err := tcpConn.Write(payload); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	if err := <-errCh; err != nil && err != io.EOF {
		log.Printf("proxy tunnel: bridge closed: %v", err)
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
