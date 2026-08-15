package wechat

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Client communicates with the WeChat iLink bot API.
type Client struct {
	httpClient *http.Client
	mu         sync.RWMutex
	baseURL    string
	botToken   string

	// sentMu protects recentSentClientIDs for echo deduplication
	sentMu              sync.Mutex
	recentSentClientIDs map[string]time.Time // clientID -> sentTime
}

// NewClient creates a new WeChat API client.
func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		recentSentClientIDs: make(map[string]time.Time),
	}
}

// SetCredentials configures the API base URL and bot token.
func (c *Client) SetCredentials(baseURL, botToken string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.baseURL = strings.TrimRight(baseURL, "/")
	c.botToken = botToken
}

func (c *Client) readCreds() (string, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.baseURL, c.botToken
}

func wechatUIN() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	u := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatUint(uint64(u), 10)))
}

func buildBaseInfo() BaseInfo {
	return BaseInfo{
		ChannelVersion: "2.4.3",
		BotAgent:       "OpenCodeGateway",
	}
}

func (c *Client) headers() http.Header {
	_, token := c.readCreds()
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("AuthorizationType", "ilink_bot_token")
	if token != "" {
		h.Set("Authorization", "Bearer "+token)
	}
	h.Set("X-WECHAT-UIN", wechatUIN())
	h.Set("iLink-App-Id", "bot")
	h.Set("iLink-App-ClientVersion", "131843")
	return h
}

func (c *Client) post(endpoint string, reqBody, respBody interface{}) error {
	base, _ := c.readCreds()
	u := fmt.Sprintf("%s/%s", base, strings.TrimLeft(endpoint, "/"))

	bodyBytes, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", u, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return err
	}
	req.Header = c.headers()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if respBody != nil {
		return json.Unmarshal(respBytes, respBody)
	}
	return nil
}

func (c *Client) postURL(fullURL string, reqBody, respBody interface{}) error {
	bodyBytes, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", fullURL, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return err
	}
	req.Header = c.headers()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if respBody != nil {
		return json.Unmarshal(respBytes, respBody)
	}
	return nil
}

func (c *Client) getURL(fullURL string, respBody interface{}) error {
	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return err
	}
	req.Header = c.headers()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, respBody)
}

// --- Core API methods ---

// --- QR Login (uses FixedBaseURL) ---

// GetBotQRCode requests a new QR code for bot login.
func (c *Client) GetBotQRCode(localTokens []string) (*GetBotQRCodeResponse, error) {
	u := fmt.Sprintf("%s/ilink/bot/get_bot_qrcode?bot_type=%s", FixedBaseURL, url.QueryEscape(DefaultBotType))
	var resp GetBotQRCodeResponse
	if err := c.postURL(u, &GetBotQRCodeRequest{LocalTokenList: localTokens}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// PollQRCodeStatus checks the status of a pending QR login.
func (c *Client) PollQRCodeStatus(qrcode, verifyCode string) (*QRCodeStatusResponse, error) {
	endpoint := fmt.Sprintf("%s/ilink/bot/get_qrcode_status?qrcode=%s", FixedBaseURL, url.QueryEscape(qrcode))
	if verifyCode != "" {
		endpoint += "&verify_code=" + url.QueryEscape(verifyCode)
	}
	var resp QRCodeStatusResponse
	if err := c.getURL(endpoint, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// --- Long-poll & messaging ---

type baseEnvelope struct {
	GetUpdatesBuf string   `json:"get_updates_buf"`
	BaseInfo      BaseInfo `json:"base_info"`
}

// GetUpdates performs a long-poll to receive new messages.
func (c *Client) GetUpdates(buf string, timeoutMs int) (*GetUpdatesResp, error) {
	if timeoutMs <= 0 {
		timeoutMs = 35000
	}
	c.httpClient.Timeout = time.Duration(timeoutMs+10000) * time.Millisecond

	body := &baseEnvelope{
		GetUpdatesBuf: buf,
		BaseInfo:      buildBaseInfo(),
	}

	var resp GetUpdatesResp
	if err := c.post("ilink/bot/getupdates", body, &resp); err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) && urlErr.Timeout() {
			return &GetUpdatesResp{Ret: 0, GetUpdatesBuf: buf}, nil
		}
		if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "Client.Timeout") {
			return &GetUpdatesResp{Ret: 0, GetUpdatesBuf: buf}, nil
		}
		return nil, err
	}
	return &resp, nil
}

type msgEnvelope struct {
	Msg      *WeixinMessage `json:"msg"`
	BaseInfo BaseInfo       `json:"base_info"`
}

// SendWeixinMessage sends a raw message via the iLink API.
func (c *Client) SendWeixinMessage(msg *WeixinMessage) error {
	// Track sent clientID for echo deduplication
	if msg.ClientID != "" {
		c.sentMu.Lock()
		c.recentSentClientIDs[msg.ClientID] = time.Now()
		// Purge entries older than 2 minutes
		for k, t := range c.recentSentClientIDs {
			if time.Since(t) > 2*time.Minute {
				delete(c.recentSentClientIDs, k)
			}
		}
		c.sentMu.Unlock()
	}

	body := &msgEnvelope{
		Msg:      msg,
		BaseInfo: buildBaseInfo(),
	}
	var resp SendMessageResp
	if err := c.post("ilink/bot/sendmessage", body, &resp); err != nil {
		return err
	}
	if resp.Ret != 0 || resp.Errcode != 0 {
		base := fmt.Errorf("sendmessage ret=%d errcode=%d errmsg=%s", resp.Ret, resp.Errcode, resp.Errmsg)
		// errcode=-14 (or ret=-14) is iLink's session-expired signal. Caller
		// should clear the cached context_token and retry once without it.
		if resp.Errcode == -14 || resp.Ret == -14 {
			return fmt.Errorf("%w: %s", ErrSessionExpired, base.Error())
		}
		// ret=-2 with an EMPTY errmsg is iLink bot rate-limit / anti-spam
		// rejection. A non-empty errmsg (e.g. "prepare failed") is NOT a rate
		// limit — the server could not prepare the conversation for this send
		// (stale/invalid context_token). Treat that like a session problem so
		// the caller clears the cached token and retries tokenless, instead of
		// feeding it into the rate-limit cooldown/park machinery.
		if resp.Ret == -2 && resp.Errcode == 0 {
			if resp.Errmsg == "" {
				return fmt.Errorf("%w: %s", ErrRateLimited, base.Error())
			}
			return fmt.Errorf("%w: %s", ErrSessionExpired, base.Error())
		}
		return base
	}
	return nil
}

// ErrRateLimited is returned (wrapped) when the iLink bot API rejects a send
// due to anti-spam / frequency limiting (ret=-2, empty errmsg).
var ErrRateLimited = errors.New("wechat rate limited")

// ErrSessionExpired is returned (wrapped) when the iLink bot API rejects a
// send because the context_token is stale (errcode=-14) or the conversation
// could not be prepared for the send (ret=-2 with a non-empty errmsg such as
// "prepare failed"). Callers should drop the cached token for the recipient
// and retry once without it.
var ErrSessionExpired = errors.New("wechat session expired")

type configEnvelope struct {
	IlinkUserID  string   `json:"ilink_user_id"`
	ContextToken string   `json:"context_token,omitempty"`
	BaseInfo     BaseInfo `json:"base_info"`
}

// GetConfig retrieves the typing ticket for a user.
func (c *Client) GetConfig(ilinkUserID, contextToken string) (*GetConfigResp, error) {
	body := &configEnvelope{
		IlinkUserID:  ilinkUserID,
		ContextToken: contextToken,
		BaseInfo:     buildBaseInfo(),
	}
	var resp GetConfigResp
	if err := c.post("ilink/bot/getconfig", body, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

type typingEnvelope struct {
	IlinkUserID  string   `json:"ilink_user_id"`
	TypingTicket string   `json:"typing_ticket"`
	Status       int      `json:"status"`
	BaseInfo     BaseInfo `json:"base_info"`
}

// SendTyping sends a typing indicator.
func (c *Client) SendTyping(ilinkUserID, typingTicket string, status int) error {
	body := &typingEnvelope{
		IlinkUserID:  ilinkUserID,
		TypingTicket: typingTicket,
		Status:       status,
		BaseInfo:     buildBaseInfo(),
	}
	return c.post("ilink/bot/sendtyping", body, nil)
}

type notifyEnvelope struct {
	BaseInfo BaseInfo `json:"base_info"`
}

// NotifyStart notifies the WeChat server that the bot is online.
func (c *Client) NotifyStart() error {
	return c.post("ilink/bot/msg/notifystart", &notifyEnvelope{BaseInfo: buildBaseInfo()}, nil)
}

// NotifyStop notifies the WeChat server that the bot is going offline.
func (c *Client) NotifyStop() error {
	return c.post("ilink/bot/msg/notifystop", &notifyEnvelope{BaseInfo: buildBaseInfo()}, nil)
}

// --- Convenience send helpers ---

var msgSeq atomic.Int64

func nextSeq() int64 {
	return msgSeq.Add(1)
}

func generateClientID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// IsSentByUs checks if a message clientID was recently sent by this bot (echo detection).
func (c *Client) IsSentByUs(clientID string) bool {
	if clientID == "" {
		return false
	}
	c.sentMu.Lock()
	defer c.sentMu.Unlock()
	_, exists := c.recentSentClientIDs[clientID]
	return exists
}

// SendText sends a plain text message to a WeChat user.
func (c *Client) SendText(toUserID, text, contextToken string) error {
	msg := &WeixinMessage{
		Seq:          int(nextSeq()),
		ToUserID:     toUserID,
		ClientID:     generateClientID(),
		ContextToken: contextToken,
		MessageType:  MessageTypeBot,
		MessageState: MessageStateFinish,
		ItemList: []MessageItem{
			{
				Type:     ItemTypeText,
				TextItem: &TextItem{Text: text},
			},
		},
	}
	return c.SendWeixinMessage(msg)
}

// SendTypingIndicator sends a typing-start indicator.
func (c *Client) SendTypingIndicator(ilinkUserID, typingTicket string) error {
	return c.SendTyping(ilinkUserID, typingTicket, TypingStatusTyping)
}

// --- Media upload ---

type getUploadURLEnvelope struct {
	FileKey     string   `json:"filekey"`
	MediaType   int      `json:"media_type"`
	ToUserID    string   `json:"to_user_id"`
	RawSize     int      `json:"rawsize"`
	RawFileMD5  string   `json:"rawfilemd5"`
	FileSize    int      `json:"filesize"`
	NoNeedThumb bool     `json:"no_need_thumb"`
	AESKey      string   `json:"aeskey"`
	BaseInfo    BaseInfo `json:"base_info"`
}

// GetUploadURL gets a CDN upload URL for sending media files.
func (c *Client) GetUploadURL(toUserID string, mediaType int, filekey, aesKeyHex string, rawSize int, rawFileMD5 string, fileSize int) (*GetUploadURLResponse, error) {
	body := &getUploadURLEnvelope{
		FileKey:     filekey,
		MediaType:   mediaType,
		ToUserID:    toUserID,
		RawSize:     rawSize,
		RawFileMD5:  rawFileMD5,
		FileSize:    fileSize,
		NoNeedThumb: true,
		AESKey:      aesKeyHex,
		BaseInfo:    buildBaseInfo(),
	}
	var resp GetUploadURLResponse
	if err := c.post("ilink/bot/getuploadurl", body, &resp); err != nil {
		return nil, err
	}
	if resp.Ret != 0 || resp.Errcode != 0 {
		return nil, fmt.Errorf("getuploadurl ret=%d errcode=%d errmsg=%s", resp.Ret, resp.Errcode, resp.Errmsg)
	}
	return &resp, nil
}

// UploadCiphertext posts encrypted bytes to the CDN and returns the encrypted query param.
func (c *Client) UploadCiphertext(uploadURL string, ciphertext []byte) (string, error) {
	bodyReader := strings.NewReader(string(ciphertext))
	req, err := http.NewRequest("POST", uploadURL, bodyReader)
	if err != nil {
		return "", err
	}

	// Set headers: start with standard headers, override Content-Type
	h := c.headers()
	h.Set("Content-Type", "application/octet-stream")
	req.Header = h
	req.ContentLength = int64(len(ciphertext))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("CDN upload: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		limit := len(raw)
		if limit > 200 {
			limit = 200
		}
		return "", fmt.Errorf("CDN upload HTTP %d: %s", resp.StatusCode, string(raw[:limit]))
	}

	encryptedParam := resp.Header.Get("x-encrypted-param")
	if encryptedParam == "" {
		raw, _ := io.ReadAll(resp.Body)
		limit := len(raw)
		if limit > 200 {
			limit = 200
		}
		return "", fmt.Errorf("CDN upload missing x-encrypted-param header: %s", string(raw[:limit]))
	}
	return encryptedParam, nil
}
