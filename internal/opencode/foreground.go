package opencode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	sdk "github.com/sst/opencode-sdk-go"
)

var ErrForegroundInteractionRequired = errors.New("opencode: foreground generation requires user interaction")

// RemoteStopUnconfirmedError prevents callers from starting a replacement
// attempt while a remote prompt may still be running.
type RemoteStopUnconfirmedError struct {
	SessionID string
	Cause     error
}

func (e *RemoteStopUnconfirmedError) Error() string {
	return fmt.Sprintf("opencode: remote stop unconfirmed for session %s: %v", e.SessionID, e.Cause)
}

func (e *RemoteStopUnconfirmedError) Unwrap() error { return e.Cause }

func (c *Client) ownsSessionHandler(sessionID string, token uint64) bool {
	c.sessionHandlersMu.Lock()
	defer c.sessionHandlersMu.Unlock()
	return c.sessionHandlerTokens[sessionID] == token
}

func (c *Client) removeForegroundHandler(sessionID string, token uint64, handler *StreamingSessionHandler, stopped bool) {
	c.sessionHandlersMu.Lock()
	defer c.sessionHandlersMu.Unlock()
	c.activeHandlers.CompareAndDelete(sessionID, handler)
	if c.sessionHandlerTokens[sessionID] != token {
		return
	}
	delete(c.sessionHandlerTokens, sessionID)
	c.sessionHandlers.Delete(sessionID)
	if stopped {
		c.runningSessions.Delete(sessionID)
		c.probeSnapshots.Delete(sessionID)
	} else {
		c.runningSessions.Store(sessionID, true)
	}
}

func (c *Client) sendMessageForeground(ctx context.Context, payload MessagePayload, callback StreamCallback, eventCallback StreamEventCallback) (response Response, resultErr error) {
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	if strings.TrimSpace(payload.Content) == "" {
		return Response{}, ErrEmptyPayload
	}
	if callback == nil {
		callback = func(string) error { return nil }
	}
	sessionID, err := c.ResolveOrCreateSession(ctx, payload)
	response.SessionID, response.Trace = sessionID, sessionID
	if err != nil {
		return response, err
	}
	if _, running := c.runningSessions.Load(sessionID); running {
		return response, &RemoteStopUnconfirmedError{SessionID: sessionID, Cause: errors.New("prior attempt still requires stop verification")}
	}
	payload.SessionID, payload.resolvedSessionID = sessionID, sessionID
	payload.Streaming = true
	completed := make(chan struct{})
	callbackErrors := make(chan error, 1)
	legacyCallback := callback
	callback = func(chunk string) error {
		err := legacyCallback(chunk)
		if err != nil {
			select {
			case callbackErrors <- err:
			default:
			}
		}
		return err
	}
	wrappedEvents := func(event StreamEvent) error {
		if eventCallback == nil {
			return nil
		}
		err := eventCallback(event)
		if err != nil {
			select {
			case callbackErrors <- err:
			default:
			}
		}
		return err
	}
	var model *sdk.SessionPromptParamsModel
	if provider, name, ok := parseProviderModelRef(payload.Model); ok {
		model = modelFromRef(provider, name)
	}
	handler := NewStreamingSessionHandlerWithModel(sessionID, callback, wrappedEvents,
		func() { close(completed) }, c, nil, false, false, model, payload)
	c.activeHandlers.Store(sessionID, handler)
	token := c.registerSessionHandler(sessionID, handler.HandleEvent)
	sendCtx, cancelSend := context.WithCancel(ctx)
	sendFinished := make(chan struct{})
	type sendResult struct {
		response Response
		err      error
	}
	sent := make(chan sendResult, 1)
	sendStarted := false
	defer func() {
		cancelSend()
		stopped := resultErr == nil
		var uncertain error
		if sendStarted {
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-sendFinished:
			case <-timer.C:
				uncertain = errors.New("prompt submission did not finish during bounded cleanup")
			}
			timer.Stop()
		}
		if resultErr != nil && sendStarted {
			if !c.ownsSessionHandler(sessionID, token) {
				uncertain = errors.Join(uncertain, errors.New("handler ownership changed before abort"))
			} else {
				abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				ok, abortErr := c.sdk.Session.Abort(abortCtx, sessionID, sdk.SessionAbortParams{})
				cancel()
				if abortErr != nil {
					uncertain = errors.Join(uncertain, abortErr)
				} else if ok == nil || !*ok {
					uncertain = errors.Join(uncertain, errors.New("server did not confirm abort"))
				} else {
					stopped = true
				}
			}
		} else if !sendStarted {
			stopped = true
		}
		if uncertain != nil {
			stopped = false
			resultErr = &RemoteStopUnconfirmedError{SessionID: sessionID, Cause: errors.Join(resultErr, uncertain)}
			log.Printf("opencode: foreground cleanup requires verification session=%s error=%v", sessionID, resultErr)
		}
		handler.mu.Lock()
		handler.completed = true
		handler.stopWaitingTimer()
		handler.mu.Unlock()
		c.removeForegroundHandler(sessionID, token, handler, stopped)
		if resultErr != nil {
			c.failRequest(generateRequestHash(payload))
		}
	}()
	if err := callback(sessionID); err != nil {
		return response, err
	}
	if err := wrappedEvents(StreamEvent{Kind: StreamEventSessionReady, SessionID: sessionID, Content: sessionID, RawType: "session"}); err != nil {
		return response, err
	}
	sendStarted = true
	go func() {
		defer close(sendFinished)
		resp, err := c.SendMessage(sendCtx, payload)
		sent <- sendResult{response: resp, err: err}
	}()
	interval := time.Second
	for _, limit := range []time.Duration{streamIdleTimeoutNoContent, streamIdleTimeoutHasContent} {
		if limit > 0 && limit/4 < interval {
			interval = max(time.Millisecond, limit/4)
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	accepted, ended := false, false
	done := (<-chan struct{})(completed)
	for {
		select {
		case <-ctx.Done():
			return response, ctx.Err()
		case err := <-callbackErrors:
			return response, fmt.Errorf("opencode: foreground event callback: %w", err)
		case result := <-sent:
			if result.err != nil {
				return response, result.err
			}
			if result.response.Deferred {
				return response, errors.New("opencode: foreground submission unexpectedly deferred")
			}
			response = result.response
			accepted = true
		case <-done:
			done = nil
			ended = true
			handler.mu.Lock()
			err := handler.terminalErr
			handler.mu.Unlock()
			if err != nil {
				return response, err
			}
		case <-ticker.C:
			handler.mu.Lock()
			lastActivity := handler.lastActivityTime
			limit := streamIdleTimeoutNoContent
			if handler.lastContent != "" {
				limit = streamIdleTimeoutHasContent
			}
			handler.mu.Unlock()
			if !ended && time.Since(lastActivity) >= limit {
				return response, fmt.Errorf("opencode: foreground session %s stalled without a terminal event", sessionID)
			}
		}
		if accepted && ended {
			select {
			case err := <-callbackErrors:
				return response, fmt.Errorf("opencode: foreground callback: %w", err)
			default:
			}
			if err := ctx.Err(); err != nil {
				return response, err
			}
			handler.mu.Lock()
			response.Reply = handler.lastContent
			err := handler.terminalErr
			handler.mu.Unlock()
			return response, err
		}
	}
}
