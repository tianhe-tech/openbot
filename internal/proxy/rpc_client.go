package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type rpcMessage struct {
	Type      string          `json:"type"`
	RequestID string          `json:"request_id"`
	Action    string          `json:"action,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// RPCHandler handles requests from hub-side HTTP API.
type RPCHandler func(ctx context.Context, action string, payload json.RawMessage) (interface{}, error)

// StartGatewayRPCClient keeps a persistent role=rpc websocket and handles request/response RPC.
func StartGatewayRPCClient(ctx context.Context, hubWSURL, proxyKey string, reconnectDelay time.Duration, handler RPCHandler) {
	if reconnectDelay <= 0 {
		reconnectDelay = 5 * time.Second
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := runRPCLoop(ctx, hubWSURL, proxyKey, handler); err != nil {
			log.Printf("proxy rpc: loop ended: %v", err)
		}
		log.Printf("proxy rpc: reconnecting in %s", reconnectDelay)
		sleepWithContext(ctx, reconnectDelay)
	}
}

func runRPCLoop(ctx context.Context, hubWSURL, proxyKey string, handler RPCHandler) error {
	rpcURL, err := buildWSURL(hubWSURL, map[string]string{
		"role":      "rpc",
		"proxy_key": proxyKey,
	})
	if err != nil {
		return fmt.Errorf("build rpc url: %w", err)
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, rpcURL, nil)
	if err != nil {
		return fmt.Errorf("dial rpc: %w", err)
	}
	defer conn.Close()

	log.Printf("proxy rpc: connected hub=%s key=%s", hubWSURL, proxyKey[:min(10, len(proxyKey))])

	var writeMu sync.Mutex
	for {
		_, body, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("rpc read: %w", err)
		}

		var req rpcMessage
		if err := json.Unmarshal(body, &req); err != nil {
			continue
		}
		if req.Type != "request" || strings.TrimSpace(req.RequestID) == "" {
			continue
		}

		go func(msg rpcMessage) {
			resp := rpcMessage{
				Type:      "response",
				RequestID: msg.RequestID,
				Action:    msg.Action,
			}

			result, err := handler(ctx, msg.Action, msg.Payload)
			if err != nil {
				resp.Error = err.Error()
			} else if result != nil {
				if data, mErr := json.Marshal(result); mErr == nil {
					resp.Payload = data
				} else {
					resp.Error = fmt.Sprintf("marshal response: %v", mErr)
				}
			}

			encoded, _ := json.Marshal(resp)
			writeMu.Lock()
			wErr := conn.WriteMessage(websocket.TextMessage, encoded)
			writeMu.Unlock()
			if wErr != nil {
				log.Printf("proxy rpc: write failed request_id=%s: %v", msg.RequestID, wErr)
			}
		}(req)
	}
}
