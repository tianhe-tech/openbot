package opencode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"

	sdk "github.com/sst/opencode-sdk-go"
)

const CronThreadPrefix = "cron-"

func isCronSessionTitle(title string) bool {
	if _, _, ok := parseSessionTitlePrefix(title); ok {
		title = title[strings.Index(title, "]")+1:]
	}
	return strings.HasPrefix(strings.TrimSpace(title), CronThreadPrefix)
}

func (c *Client) SetCronSessionChecker(checker func(string) bool) {
	c.cronSessionCheckerMu.Lock()
	defer c.cronSessionCheckerMu.Unlock()
	c.cronSessionChecker = checker
}

func (c *Client) isCronSession(sessionID, title string) bool {
	c.cronSessionCheckerMu.RLock()
	checker := c.cronSessionChecker
	c.cronSessionCheckerMu.RUnlock()
	return isCronSessionTitle(title) || (checker != nil && checker(sessionID))
}

func (c *Client) selectableSession(session UserSession) (UserSession, error) {
	if c.isCronSession(session.ID, session.Title) {
		return UserSession{}, fmt.Errorf("opencode: cron session %s cannot become the current chat session", session.ID)
	}
	return session, nil
}

// ResolveOrCreateSession resolves without sending a prompt. The hook must commit
// any durable binding before the caller is allowed to dispatch into the session.
func (c *Client) ResolveOrCreateSession(ctx context.Context, payload MessagePayload) (string, error) {
	if !c.Ready() {
		return "", fmt.Errorf("opencode: client not configured")
	}
	if err := c.checkExplicitSessionOwnership(payload); err != nil {
		return "", err
	}
	lock := c.getThreadLock(payload.ThreadID)
	lock.Lock()
	defer lock.Unlock()

	sessionID := payload.SessionID
	if sessionID == "" && payload.ThreadID != "" {
		sessionID, _ = c.resolveThreadSession(payload.ThreadID, payload.Channel, payload.UserID)
	}
	title := fmt.Sprintf("[%s:%s] %s", payload.Channel, payload.UserID, payload.ThreadID)
	cron := strings.HasPrefix(payload.ThreadID, CronThreadPrefix)
	if cron && (strings.TrimSpace(payload.Channel) == "" || strings.TrimSpace(payload.UserID) == "") {
		return "", fmt.Errorf("opencode: cron session requires a channel and owner")
	}
	if cron && sessionID == "" {
		sessions, err := c.listSessions(ctx, title)
		if err != nil {
			return "", fmt.Errorf("opencode: recover cron session: %w", err)
		}
		for _, session := range sessions {
			if session.Title != title {
				continue
			}
			if sessionID != "" {
				return "", fmt.Errorf("opencode: multiple sessions for cron thread %s", payload.ThreadID)
			}
			sessionID = session.ID
		}
	}
	if sessionID != "" {
		session, err := c.GetSession(ctx, sessionID)
		if err != nil {
			var apiErr *sdk.Error
			if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
				return "", fmt.Errorf("opencode: validate session %s: %w", sessionID, err)
			}
			c.sessions.Delete(payload.ThreadID)
			c.messageCount.Delete(sessionID)
			c.toolCallCount.Delete(sessionID)
			c.tokenCount.Delete(sessionID)
			c.modelConfig.Delete(sessionID)
			c.modelOverride.Delete(sessionID)
			sessionID = ""
		} else {
			if session == nil {
				return "", fmt.Errorf("opencode: empty session response for %s", sessionID)
			}
			if cron && session.Title != title {
				return "", fmt.Errorf("opencode: session %s is not owned by cron thread %s", sessionID, payload.ThreadID)
			}
		}
	}
	if sessionID == "" {
		session, err := c.sdk.Session.New(ctx, sdk.SessionNewParams{Title: sdk.F(title)})
		if err != nil {
			return "", fmt.Errorf("opencode: create session: %w", err)
		}
		if session == nil || strings.TrimSpace(session.ID) == "" {
			return "", fmt.Errorf("opencode: create session returned no session ID")
		}
		sessionID = session.ID
		c.messageCount.Store(sessionID, 0)
		c.toolCallCount.Store(sessionID, 0)
		c.tokenCount.Store(sessionID, 0)
		go c.fetchAndCacheModelConfig(context.Background(), sessionID)
		log.Printf("opencode: created session %s for thread %s", sessionID, payload.ThreadID)
	}
	if payload.ThreadID != "" {
		c.sessions.Store(payload.ThreadID, sessionID)
	}
	if payload.OnSessionResolved != nil {
		if err := payload.OnSessionResolved(ctx, sessionID); err != nil {
			return sessionID, fmt.Errorf("opencode: commit session %s: %w", sessionID, err)
		}
	}
	return sessionID, nil
}
