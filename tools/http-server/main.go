package main

import (
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// controlMsg is sent from hub → gateway over the control WS to request a new tunnel.
type controlMsg struct {
	Action string `json:"action"` // "connect"
	Token  string `json:"token"`  // one-time session token
}

type controlConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

type hub struct {
	mu       sync.Mutex
	controls map[string]*controlConn         // proxy_key → persistent control WS (gateway→hub)
	pending  map[string]chan *websocket.Conn // session token → channel waiting for data WS
}

func newHub() *hub {
	return &hub{
		controls: make(map[string]*controlConn),
		pending:  make(map[string]chan *websocket.Conn),
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	addr := flag.String("addr", ":18080", "http listen address")
	flag.Parse()

	h := newHub()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ws", h.handleWS)

	log.Printf("http server listening on %s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("http server stopped: %v", err)
	}
}

func (h *hub) handleWS(w http.ResponseWriter, r *http.Request) {
	role := r.URL.Query().Get("role")
	proxyKey := r.URL.Query().Get("proxy_key")

	switch role {
	case "control":
		// Persistent control connection from gateway.
		if proxyKey == "" {
			http.Error(w, "missing proxy_key", http.StatusBadRequest)
			return
		}
		h.handleControl(w, r, proxyKey)

	case "data":
		// Per-connection data tunnel dialled by gateway on demand.
		token := r.URL.Query().Get("token")
		if token == "" {
			http.Error(w, "missing token", http.StatusBadRequest)
			return
		}
		h.handleData(w, r, token)

	case "client":
		// Local opencode client (via client-proxy).
		if proxyKey == "" {
			http.Error(w, "missing proxy_key", http.StatusBadRequest)
			return
		}
		h.handleClient(w, r, proxyKey)

	default:
		http.Error(w, "invalid or missing role", http.StatusBadRequest)
	}
}

// handleControl manages the persistent control WebSocket from the gateway.
// Hub writes JSON controlMsg commands; gateway responds by dialling a data WS.
// Reading is the sole job of this goroutine — no bridge reader ever touches
// this conn, so there are no concurrent-read issues.
func (h *hub) handleControl(w http.ResponseWriter, r *http.Request, proxyKey string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade control failed: %v", err)
		return
	}
	defer conn.Close()
	ctrl := &controlConn{conn: conn}

	h.mu.Lock()
	if old, ok := h.controls[proxyKey]; ok {
		_ = old.conn.Close()
	}
	h.controls[proxyKey] = ctrl
	h.mu.Unlock()

	log.Printf("control registered: key=%s", shortKey(proxyKey))

	// Keep the control conn alive with pong responses.
	conn.SetPingHandler(func(data string) error {
		ctrl.writeMu.Lock()
		defer ctrl.writeMu.Unlock()
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})

	// Drain any incoming messages so the connection stays healthy.
	// The gateway currently sends nothing on the control WS, but we must
	// read to process close / ping frames.
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}

	h.mu.Lock()
	if h.controls[proxyKey] == ctrl {
		delete(h.controls, proxyKey)
	}
	h.mu.Unlock()
	log.Printf("control disconnected: key=%s", shortKey(proxyKey))
}

// handleData is called when the gateway dials a new data WS in response to a
// controlMsg. It delivers the conn to the waiting handleClient via the pending map.
func (h *hub) handleData(w http.ResponseWriter, r *http.Request, token string) {
	h.mu.Lock()
	ch, ok := h.pending[token]
	h.mu.Unlock()
	if !ok {
		http.Error(w, "unknown token", http.StatusNotFound)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade data failed: %v", err)
		return
	}

	// Deliver to handleClient. gorilla has hijacked the TCP conn so returning
	// from this handler is safe — http.Server no longer owns the conn.
	ch <- conn
}

// handleClient is called when the local opencode client (via client-proxy) connects.
// It asks the gateway to open a fresh data WS, then bridges both sides.
func (h *hub) handleClient(w http.ResponseWriter, r *http.Request, proxyKey string) {
	h.mu.Lock()
	ctrlConn, ok := h.controls[proxyKey]
	if !ok {
		h.mu.Unlock()
		http.Error(w, "gateway not connected", http.StatusNotFound)
		return
	}

	// Register a pending channel BEFORE sending the control message to avoid
	// a race where the data WS arrives before we're listening.
	token := newToken()
	ch := make(chan *websocket.Conn, 1)
	h.pending[token] = ch
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.pending, token)
		h.mu.Unlock()
	}()

	// Send "connect" command to gateway.
	msg, _ := json.Marshal(controlMsg{Action: "connect", Token: token})
	ctrlConn.writeMu.Lock()
	err := ctrlConn.conn.WriteMessage(websocket.TextMessage, msg)
	ctrlConn.writeMu.Unlock()
	if err != nil {
		http.Error(w, "control write failed", http.StatusServiceUnavailable)
		log.Printf("control write failed: key=%s %v", shortKey(proxyKey), err)
		return
	}

	log.Printf("waiting for data WS: key=%s token=%s", shortKey(proxyKey), token[:8])

	// Wait for the gateway to dial back with the data WS.
	var gwConn *websocket.Conn
	select {
	case gwConn = <-ch:
	case <-time.After(10 * time.Second):
		http.Error(w, "gateway did not connect in time", http.StatusGatewayTimeout)
		log.Printf("timeout waiting for data WS: key=%s token=%s", shortKey(proxyKey), token[:8])
		return
	}

	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		_ = gwConn.Close()
		log.Printf("upgrade client failed: %v", err)
		return
	}

	log.Printf("client paired: key=%s token=%s", shortKey(proxyKey), token[:8])
	go func() {
		bridgeWSToWS(gwConn, clientConn)
		log.Printf("session ended: key=%s token=%s", shortKey(proxyKey), token[:8])
	}()
}

func bridgeWSToWS(gwConn, clientConn *websocket.Conn) {
	defer gwConn.Close()
	defer clientConn.Close()

	errCh := make(chan error, 2)
	go pipeWS(gwConn, clientConn, errCh)
	go pipeWS(clientConn, gwConn, errCh)

	if err := <-errCh; err != nil && err != io.EOF {
		log.Printf("bridge closed: %v", err)
	}
}

func pipeWS(src, dst *websocket.Conn, errCh chan<- error) {
	for {
		messageType, payload, err := src.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		if err := dst.WriteMessage(messageType, payload); err != nil {
			errCh <- err
			return
		}
	}
}

func shortKey(k string) string {
	if len(k) <= 10 {
		return k
	}
	return k[:10]
}

func newToken() string {
	b := make([]byte, 16)
	_, _ = crand.Read(b)
	return hex.EncodeToString(b)
}
