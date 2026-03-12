package main

import (
	crand "crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
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

type rpcConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

type rpcMessage struct {
	Type      string          `json:"type"` // "request" | "response"
	RequestID string          `json:"request_id"`
	Action    string          `json:"action,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Error     string          `json:"error,omitempty"`
}

type proxyRequest struct {
	ProxyKey  string          `json:"proxy_key,omitempty"`
	SessionID string          `json:"session_id,omitempty"`
	Action    string          `json:"action"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timeout   int             `json:"timeout_seconds,omitempty"`
}

type connectRequest struct {
	ProxyKey  string `json:"proxy_key"`
	SessionID string `json:"session_id,omitempty"`
}

type connectResponse struct {
	OK        bool   `json:"ok"`
	SessionID string `json:"session_id,omitempty"`
	ProxyKey  string `json:"proxy_key,omitempty"`
	Message   string `json:"message,omitempty"`
	Error     string `json:"error,omitempty"`
}

type proxyResponse struct {
	OK       bool            `json:"ok"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Error    string          `json:"error,omitempty"`
	ProxyKey string          `json:"proxy_key,omitempty"`
	Action   string          `json:"action,omitempty"`
}

type hub struct {
	mu         sync.Mutex
	controls   map[string]*controlConn         // proxy_key → persistent control WS (gateway→hub)
	rpcs       map[string]*rpcConn             // proxy_key → persistent RPC WS (gateway→hub)
	pending    map[string]chan *websocket.Conn // session token → channel waiting for data WS
	rpcPending map[string]chan rpcMessage      // request_id → channel waiting for RPC response
	uiSessions map[string]string               // ui session id -> proxy_key
}

func newHub() *hub {
	return &hub{
		controls:   make(map[string]*controlConn),
		rpcs:       make(map[string]*rpcConn),
		pending:    make(map[string]chan *websocket.Conn),
		rpcPending: make(map[string]chan rpcMessage),
		uiSessions: make(map[string]string),
	}
}

//go:embed ui/*
var uiFS embed.FS

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
	mux.HandleFunc("/api/proxy/request", h.handleProxyRequest)
	mux.HandleFunc("/api/proxy/keys", h.handleProxyKeys)
	mux.HandleFunc("/api/session/connect", h.handleSessionConnect)
	mux.HandleFunc("/ws", h.handleWS)
	mux.Handle("/ui/", http.StripPrefix("/ui/", http.FileServer(http.FS(uiFS))))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/ui/index.html", http.StatusFound)
	})

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

	case "rpc":
		if proxyKey == "" {
			http.Error(w, "missing proxy_key", http.StatusBadRequest)
			return
		}
		h.handleRPC(w, r, proxyKey)

	default:
		http.Error(w, "invalid or missing role", http.StatusBadRequest)
	}
}

func (h *hub) handleProxyRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req proxyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, proxyResponse{OK: false, Error: "invalid json"})
		return
	}

	req.ProxyKey = strings.TrimSpace(req.ProxyKey)
	req.SessionID = strings.TrimSpace(req.SessionID)
	req.Action = strings.TrimSpace(req.Action)
	if req.Action == "" {
		writeJSON(w, http.StatusBadRequest, proxyResponse{OK: false, Error: "action is required"})
		return
	}

	if req.ProxyKey == "" && req.SessionID != "" {
		h.mu.Lock()
		if key, ok := h.uiSessions[req.SessionID]; ok {
			req.ProxyKey = key
		}
		h.mu.Unlock()
	}

	if req.ProxyKey == "" {
		writeJSON(w, http.StatusBadRequest, proxyResponse{OK: false, Error: "proxy_key or valid session_id is required"})
		return
	}

	h.mu.Lock()
	rpc, ok := h.rpcs[req.ProxyKey]
	if !ok {
		h.mu.Unlock()
		writeJSON(w, http.StatusNotFound, proxyResponse{OK: false, Error: "gateway rpc not connected", ProxyKey: req.ProxyKey, Action: req.Action})
		return
	}

	requestID := newToken()
	respCh := make(chan rpcMessage, 1)
	h.rpcPending[requestID] = respCh
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.rpcPending, requestID)
		h.mu.Unlock()
	}()

	out, _ := json.Marshal(rpcMessage{
		Type:      "request",
		RequestID: requestID,
		Action:    req.Action,
		Payload:   req.Payload,
	})

	rpc.writeMu.Lock()
	writeErr := rpc.conn.WriteMessage(websocket.TextMessage, out)
	rpc.writeMu.Unlock()
	if writeErr != nil {
		writeJSON(w, http.StatusServiceUnavailable, proxyResponse{OK: false, Error: "rpc write failed", ProxyKey: req.ProxyKey, Action: req.Action})
		return
	}

	timeout := 25 * time.Second
	if req.Timeout > 0 && req.Timeout <= 120 {
		timeout = time.Duration(req.Timeout) * time.Second
	}

	select {
	case resp := <-respCh:
		if strings.TrimSpace(resp.Error) != "" {
			writeJSON(w, http.StatusBadRequest, proxyResponse{OK: false, Error: resp.Error, ProxyKey: req.ProxyKey, Action: req.Action})
			return
		}
		writeJSON(w, http.StatusOK, proxyResponse{OK: true, Payload: resp.Payload, ProxyKey: req.ProxyKey, Action: req.Action})
	case <-time.After(timeout):
		writeJSON(w, http.StatusGatewayTimeout, proxyResponse{OK: false, Error: "rpc timeout", ProxyKey: req.ProxyKey, Action: req.Action})
	}
}

func (h *hub) handleSessionConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req connectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, connectResponse{OK: false, Error: "invalid json"})
		return
	}

	req.ProxyKey = strings.TrimSpace(req.ProxyKey)
	req.SessionID = strings.TrimSpace(req.SessionID)
	if req.ProxyKey == "" {
		writeJSON(w, http.StatusBadRequest, connectResponse{OK: false, Error: "proxy_key is required"})
		return
	}

	h.mu.Lock()
	_, ok := h.rpcs[req.ProxyKey]
	h.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, connectResponse{OK: false, Error: "target openbot rpc websocket not connected"})
		return
	}

	if req.SessionID == "" {
		req.SessionID = "ui-" + newToken()
	}

	h.mu.Lock()
	h.uiSessions[req.SessionID] = req.ProxyKey
	h.mu.Unlock()

	writeJSON(w, http.StatusOK, connectResponse{
		OK:        true,
		SessionID: req.SessionID,
		ProxyKey:  req.ProxyKey,
		Message:   "connected",
	})
}

func (h *hub) handleProxyKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.mu.Lock()
	keys := make([]string, 0, len(h.rpcs))
	for k := range h.rpcs {
		keys = append(keys, k)
	}
	h.mu.Unlock()

	sort.Strings(keys)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"ok":   true,
		"keys": keys,
	})
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

func (h *hub) handleRPC(w http.ResponseWriter, r *http.Request, proxyKey string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade rpc failed: %v", err)
		return
	}
	rpc := &rpcConn{conn: conn}

	h.mu.Lock()
	if old, ok := h.rpcs[proxyKey]; ok {
		_ = old.conn.Close()
	}
	h.rpcs[proxyKey] = rpc
	h.mu.Unlock()

	log.Printf("rpc registered: key=%s", shortKey(proxyKey))

	defer func() {
		_ = conn.Close()
		h.mu.Lock()
		if h.rpcs[proxyKey] == rpc {
			delete(h.rpcs, proxyKey)
		}
		h.mu.Unlock()
		log.Printf("rpc disconnected: key=%s", shortKey(proxyKey))
	}()

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var msg rpcMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}
		if msg.Type != "response" || strings.TrimSpace(msg.RequestID) == "" {
			continue
		}

		h.mu.Lock()
		ch, ok := h.rpcPending[msg.RequestID]
		h.mu.Unlock()
		if ok {
			select {
			case ch <- msg:
			default:
			}
		}
	}
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

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
