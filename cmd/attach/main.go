// cmd/attach is a thin wrapper that starts an in-process client-proxy and
// then runs "opencode attach <local_addr>" transparently.
//
// Usage:
//
//	opencode-attach <hub_url> <proxy_key> [--listen 127.0.0.1:14096]
//
// Example:
//
//	opencode-attach http://hub.example.com:18080 abc123
//
// The tool will:
//  1. Start a local TCP listener (default 127.0.0.1:14096)
//  2. For every TCP connection, dial the hub WS with role=client and bridge them
//  3. Run "opencode attach http://127.0.0.1:14096" with inherited stdio
//  4. Exit with opencode's exit code when it exits
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:14096", "local tcp listen address forwarded to opencode attach")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [--listen addr] <hub_url> <proxy_key>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nExample:\n  %s http://hub:18080 abc123def456\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		flag.Usage()
		os.Exit(1)
	}
	hubURL := args[0]
	proxyKey := args[1]

	wsURL, err := buildClientURL(hubURL, proxyKey)
	if err != nil {
		log.Fatalf("invalid hub url %q: %v", hubURL, err)
	}

	// Start local TCP listener.
	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen %s failed: %v", *listenAddr, err)
	}

	log.Printf("attach proxy: listening on %s → hub %s", *listenAddr, hubURL)

	// Accept connections in background goroutines.
	go func() {
		for {
			tc, err := ln.Accept()
			if err != nil {
				// listener closed (process exiting)
				return
			}
			go handleConn(tc, wsURL)
		}
	}()

	// Build the opencode attach command, inheriting our full environment.
	localEndpoint := "http://" + *listenAddr
	cmd := exec.Command("opencode", "attach", localEndpoint)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()

	log.Printf("attach proxy: running: opencode attach %s", localEndpoint)

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			ln.Close()
			os.Exit(exitErr.ExitCode())
		}
		log.Printf("attach proxy: opencode exited: %v", err)
		ln.Close()
		os.Exit(1)
	}

	ln.Close()
}

func handleConn(tcpConn net.Conn, wsURL string) {
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		_ = tcpConn.Close()
		log.Printf("attach proxy: dial hub failed: %v", err)
		return
	}

	defer tcpConn.Close()
	defer wsConn.Close()

	bridgeTCPWS(tcpConn, wsConn)
}

func buildClientURL(rawHubURL, proxyKey string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(rawHubURL))
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
		// already correct
	default:
		return "", fmt.Errorf("unsupported scheme %q (use http/https/ws/wss)", u.Scheme)
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
	var wsMu sync.Mutex

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
		log.Printf("attach proxy: session closed: %v", err)
	}
}
