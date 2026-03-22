package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	AICardTemplateID = "02fcf2f4-5e02-4a85-b672-46d1f715543e.schema"
)

type CardSendConfig struct {
	Enabled        bool
	AutoDowngrade  bool
	StreamInterval int
	MinChunkSize   int
}

func getCardSendConfig() CardSendConfig {
	return CardSendConfig{
		Enabled:        getEnvBool("DINGTALK_CARD_ENABLED", true),
		AutoDowngrade:  getEnvBool("DINGTALK_CARD_AUTO_DOWNGRADE", true),
		StreamInterval: getEnvInt("DINGTALK_CARD_STREAM_INTERVAL", 200),
		MinChunkSize:   getEnvInt("DINGTALK_CARD_MIN_CHUNK_SIZE", 20),
	}
}

func getEnvBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
}

func getEnvInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func ensureTableBlankLines(text string) string {
	lines := strings.Split(text, "\n")
	var result []string
	for i, line := range lines {
		if strings.Contains(line, "|") && i+1 < len(lines) && strings.Contains(lines[i+1], "-") && strings.Contains(lines[i+1], "|") {
			if i > 0 && strings.TrimSpace(lines[i-1]) != "" && !strings.Contains(lines[i-1], "|") {
				result = append(result, "")
			}
		}
		result = append(result, line)
	}
	return strings.Join(result, "\n")
}

func SendStreamingAICard(ctx context.Context, clientID, clientSecret, userID string, content string) error {
	cfg := getCardSendConfig()
	if !cfg.Enabled {
		return fmt.Errorf("aicard is disabled by config")
	}
	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(clientSecret) == "" {
		return fmt.Errorf("missing dingtalk client credentials for aicard")
	}
	if strings.TrimSpace(userID) == "" {
		return fmt.Errorf("missing dingtalk user id for aicard")
	}
	if strings.TrimSpace(content) == "" {
		return fmt.Errorf("empty content for aicard")
	}

	token, err := getAccessToken(ctx, clientID, clientSecret)
	if err != nil {
		return fmt.Errorf("get access token: %w", err)
	}
	outTrackID := fmt.Sprintf("aicard-%d", time.Now().UnixNano())
	card, err := createAICard(ctx, token, outTrackID)
	if err != nil {
		return fmt.Errorf("create ai card: %w", err)
	}
	log.Printf("dingtalk: ✓ Card created: %s", card.CardInstanceID)
	if err := deliverAICard(ctx, token, card.CardInstanceID, userID, clientID); err != nil {
		return fmt.Errorf("deliver ai card: %w", err)
	}
	log.Printf("dingtalk: ✓ Card delivered to: %s", userID)
	if err := switchToInputing(ctx, token, card.CardInstanceID); err != nil {
		return fmt.Errorf("switch ai card to inputing: %w", err)
	}
	log.Printf("dingtalk: ✓ Switched to INPUTING")

	runes := []rune(content)
	chunk := cfg.MinChunkSize
	if chunk <= 0 {
		chunk = 20
	}
	for i := chunk; i <= len(runes); i += chunk {
		txt := ensureTableBlankLines(string(runes[:i]))
		if err := streamUpdate(ctx, token, card.CardInstanceID, txt, false); err != nil {
			return fmt.Errorf("stream update failed at %d chars: %w", i, err)
		}
		time.Sleep(time.Duration(cfg.StreamInterval) * time.Millisecond)
	}

	final := ensureTableBlankLines(content)
	if err := finishCard(ctx, token, card.CardInstanceID, final); err != nil {
		return fmt.Errorf("finish ai card: %w", err)
	}
	log.Printf("dingtalk: ✓ Streaming completed, len=%d", len(final))
	return nil
}

func createAICard(ctx context.Context, token, outTrackID string) (*struct{ CardInstanceID string }, error) {
	body := map[string]interface{}{
		"cardTemplateId":        AICardTemplateID,
		"outTrackId":            outTrackID,
		"cardData":              map[string]interface{}{"cardParamMap": map[string]string{"config": `{"autoLayout":true}`}},
		"callbackType":          "STREAM",
		"imRobotOpenSpaceModel": map[string]bool{"supportForward": true},
	}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.dingtalk.com/v1.0/card/instances", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("create card status %d: %s", resp.StatusCode, string(raw))
	}
	if err := checkDingTalkAPIError(raw); err != nil {
		return nil, fmt.Errorf("create card api error: %w", err)
	}
	var result struct {
		Success bool
		Result  string
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode create card response: %w", err)
	}
	if strings.TrimSpace(result.Result) == "" {
		return nil, fmt.Errorf("create card returned empty result: %s", string(raw))
	}
	return &struct{ CardInstanceID string }{CardInstanceID: result.Result}, nil
}

func deliverAICard(ctx context.Context, token, outTrackID, userID, robotCode string) error {
	body := map[string]interface{}{
		"outTrackId": outTrackID, "userId": userID, "userIdType": 1,
		"openSpaceId":             fmt.Sprintf("dtv1.card//IM_ROBOT.%s", userID),
		"imRobotOpenDeliverModel": map[string]string{"robotCode": robotCode},
	}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.dingtalk.com/v1.0/card/instances/deliver", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deliver card status %d: %s", resp.StatusCode, string(raw))
	}
	if err := checkDingTalkAPIError(raw); err != nil {
		return fmt.Errorf("deliver card api error: %w", err)
	}
	return nil
}

func switchToInputing(ctx context.Context, token, outTrackID string) error {
	body := map[string]interface{}{
		"outTrackId": outTrackID,
		"cardData": map[string]interface{}{
			"cardParamMap": map[string]string{"flowStatus": "2", "msgContent": "", "staticMsgContent": "", "sys_full_json_obj": `{"order":["msgContent"]}`},
		},
	}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, "https://api.dingtalk.com/v1.0/card/instances", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("switch card status %d: %s", resp.StatusCode, string(raw))
	}
	if err := checkDingTalkAPIError(raw); err != nil {
		return fmt.Errorf("switch card api error: %w", err)
	}
	return nil
}

func streamUpdate(ctx context.Context, token, outTrackID, content string, finalize bool) error {
	body := map[string]interface{}{
		"outTrackId": outTrackID, "guid": fmt.Sprintf("s%d", time.Now().UnixNano()),
		"key": "msgContent", "content": content, "isFull": true, "isFinalize": finalize,
	}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, "https://api.dingtalk.com/v1.0/card/streaming", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stream update status %d: %s", resp.StatusCode, string(raw))
	}
	if err := checkDingTalkAPIError(raw); err != nil {
		return fmt.Errorf("stream update api error: %w", err)
	}
	return nil
}

func finishCard(ctx context.Context, token, outTrackID, content string) error {
	if err := streamUpdate(ctx, token, outTrackID, content, true); err != nil {
		return err
	}
	body := map[string]interface{}{
		"outTrackId": outTrackID,
		"cardData": map[string]interface{}{
			"cardParamMap":      map[string]string{"flowStatus": "3", "msgContent": content, "staticMsgContent": content, "sys_full_json_obj": `{"order":["msgContent"]}`},
			"cardUpdateOptions": map[string]bool{"updateCardDataByKey": true},
		},
	}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, "https://api.dingtalk.com/v1.0/card/instances", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("finish card status %d: %s", resp.StatusCode, string(raw))
	}
	if err := checkDingTalkAPIError(raw); err != nil {
		return fmt.Errorf("finish card api error: %w", err)
	}
	return nil
}

func getAccessToken(ctx context.Context, clientID, clientSecret string) (string, error) {
	body := map[string]string{"appKey": clientID, "appSecret": clientSecret}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.dingtalk.com/v1.0/oauth2/accessToken", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get access token status %d: %s", resp.StatusCode, string(raw))
	}
	if err := checkDingTalkAPIError(raw); err != nil {
		return "", fmt.Errorf("get access token api error: %w", err)
	}
	var result struct{ AccessToken string }
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("decode access token response: %w", err)
	}
	if strings.TrimSpace(result.AccessToken) == "" {
		return "", fmt.Errorf("empty access token in response: %s", string(raw))
	}
	return result.AccessToken, nil
}

func checkDingTalkAPIError(raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	if payload.ErrCode != 0 {
		if payload.ErrMsg == "" {
			payload.ErrMsg = string(raw)
		}
		return fmt.Errorf("errcode=%d errmsg=%s", payload.ErrCode, payload.ErrMsg)
	}
	if payload.Code != "" && payload.Code != "0" {
		if payload.Message == "" {
			payload.Message = string(raw)
		}
		return fmt.Errorf("code=%s message=%s", payload.Code, payload.Message)
	}
	return nil
}
