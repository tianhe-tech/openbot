package opencode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

// ErrEmptyPayload indicates the caller attempted to send an empty message.
var ErrEmptyPayload = errors.New("opencode: empty payload")

// ErrDuplicateRequest indicates a duplicate request was detected.
var ErrDuplicateRequest = errors.New("opencode: duplicate request detected")

// ErrMaxRetriesExceeded indicates all retry attempts failed.
var ErrMaxRetriesExceeded = errors.New("opencode: max retries exceeded")

const (
	// ContextUsageThreshold 涓婁笅鏂囦娇鐢ㄧ巼杈惧埌姝ら槇鍊兼椂鍒涘缓鏂皊ession (榛樿80%)
	ContextUsageThreshold = 0.8
	// SummaryThreshold 涓婁笅鏂囦娇鐢ㄧ巼杈惧埌姝ら槇鍊兼椂寮€濮嬫€荤粨 (榛樿60%)
	SummaryThreshold = 0.6
	// DefaultMaxTokens 榛樿鏈€澶oken鏁帮紙褰撴棤娉曡幏鍙栨ā鍨嬮厤缃椂浣跨敤锛?
	DefaultMaxTokens = 4096
	// EstimatedTokensPerMessage 浼扮畻姣忔潯娑堟伅鐨勫钩鍧噒oken鏁?
	EstimatedTokensPerMessage = 200
	// FallbackMaxMessages 闄嶇骇鏂规锛氭寜娑堟伅鏁板垽鏂紙褰搕oken浼扮畻涓嶅彲鐢ㄦ椂锛?
	FallbackMaxMessages = 50

	// MaxRetries 鏈€澶ч噸璇曟鏁?
	MaxRetries = 3
	// InitialRetryDelay 鍒濆閲嶈瘯寤惰繜
	InitialRetryDelay = 2 * time.Second
	// MaxRetryDelay 鏈€澶ч噸璇曞欢杩?
	MaxRetryDelay = 30 * time.Second
	// RequestDeduplicationWindow 璇锋眰鍘婚噸鏃堕棿绐楀彛锛堝彧闃叉蹇€熼噸澶嶇偣鍑伙級
	RequestDeduplicationWindow = 30 * time.Second
)

// Response represents the minimal data we expect back from OpenCode.
type Response struct {
	Reply     string                 `json:"reply"`
	Trace     string                 `json:"trace_id"`
	SessionID string                 `json:"session_id"`
	MessageID string                 `json:"message_id"`
	Raw       map[string]interface{} `json:"raw,omitempty"`
}

// Attachment 琛ㄧず闄勪欢锛堝浘鐗囥€佽闊炽€佽棰戠瓑濯掍綋鏂囦欢锛?
// URL 蹇呴』鏄?data URI 鏍煎紡锛歞ata:<mime>;base64,<base64data>
type Attachment struct {
	Mime     string `json:"mime"`               // MIME 绫诲瀷锛屽 image/jpeg銆乮mage/png
	URL      string `json:"url"`                // data URI: data:<mime>;base64,<base64>
	Filename string `json:"filename,omitempty"` // 鍙€夋枃浠跺悕
}

// MessagePayload collects the metadata adapters send to OpenCode.
type MessagePayload struct {
	Channel     string            `json:"channel"`
	UserID      string            `json:"user_id"`
	ThreadID    string            `json:"thread_id,omitempty"`
	SessionID   string            `json:"session_id,omitempty"`
	Content     string            `json:"content"`
	Agent       string            `json:"agent,omitempty"`     // 鍙€夛細鎸囧畾浣跨敤鐨刟gent/skill鍚嶇О
	Streaming   bool              `json:"streaming,omitempty"` // 鏄惁浣跨敤娴佸紡杩斿洖
	Metadata    map[string]string `json:"metadata,omitempty"`
	Attachments []Attachment      `json:"attachments,omitempty"` // 闄勪欢锛堝浘鐗?璇煶/瑙嗛锛?
}

// StreamCallback defines a callback for streaming responses.
type StreamCallback func(chunk string) error

// EventHandler defines a callback for incoming OpenCode events.
type EventHandler func(ctx context.Context, event *opencode.EventListResponse) error

// ServerUnavailableHandler is invoked when OpenCode server is unreachable.
// Typical use is to ensure `opencode serve` is running.
type ServerUnavailableHandler func(ctx context.Context, reason string) (string, error)

// ModelConfig 瀛樺偍妯″瀷閰嶇疆淇℃伅
type ModelConfig struct {
	ProviderID    string // 鎻愪緵鍟咺D (濡?"anthropic", "openai")
	ModelID       string // 妯″瀷ID (濡?"claude-3-opus", "gpt-4")
	ContextLength int    // 妯″瀷涓婁笅鏂囬暱搴?
	LastUpdated   time.Time
}

// ModelCapability stores model input/output modality capabilities.
type ModelCapability struct {
	ProviderID       string
	ModelID          string
	InputModalities  map[string]bool
	OutputModalities map[string]bool
}

// RequestRecord 璁板綍宸插鐞嗙殑璇锋眰鐢ㄤ簬鍘婚噸
type RequestRecord struct {
	Hash      string
	Response  Response
	Timestamp time.Time
	InFlight  bool // 鏄惁姝ｅ湪澶勭悊涓?
}

// RetryConfig 閲嶈瘯閰嶇疆
type RetryConfig struct {
	MaxRetries      int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	RetryableErrors []string // 鍙噸璇曠殑閿欒绫诲瀷鍏抽敭瀛?
}

// QuestionOption 琛ㄧず闂閫夐」
type QuestionOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

// QuestionItem 琛ㄧず鍗曚釜瀛愰棶棰?
type QuestionItem struct {
	Header   string           `json:"header"`
	Question string           `json:"question"`
	Multiple bool             `json:"multiple"`
	Options  []QuestionOption `json:"options"`
}

// TodoItem represents a single TODO item tracked by OpenCode during a session.
// Mirrors the "todo.updated" SSE event structure from the OpenCode server.
type TodoItem struct {
	ID       string `json:"id"`
	Task     string `json:"task"`     // 鍏煎鏃у瓧娈?
	Content  string `json:"content"`  // OpenCode SDK todo.updated 褰撳墠瀛楁
	Status   string `json:"status"`   // "pending", "in_progress", "completed", "cancelled"
	Priority string `json:"priority"` // "high", "medium", "low"
}

// Text returns the human-readable todo text with backward/forward compatibility.
func (t TodoItem) Text() string {
	if strings.TrimSpace(t.Content) != "" {
		return t.Content
	}
	return t.Task
}

// PriorityLabel returns a user-friendly priority label.
func (t TodoItem) PriorityLabel() string {
	switch strings.ToLower(strings.TrimSpace(t.Priority)) {
	case "high":
		return "high"
	case "medium":
		return "medium"
	case "low":
		return "low"
	default:
		if strings.TrimSpace(t.Priority) == "" {
			return "unset"
		}
		return t.Priority
	}
}

// FileDiff represents changes to a single file within a session.
// Mirrors the "session.diff" SSE event structure from the OpenCode server.
type FileDiff struct {
	Path    string `json:"path"`
	Added   int    `json:"added"`
	Removed int    `json:"removed"`
}

// Question represents a pending question that needs user confirmation
type Question struct {
	ID           string         `json:"id"`
	SessionID    string         `json:"session_id"`
	MessageID    string         `json:"message_id"`
	Text         string         `json:"text"`          // 绠€鍖栫殑闂鏂囨湰锛堝悜鍚庡吋瀹癸級
	Options      []string       `json:"options"`       // 绠€鍖栫殑閫夐」鍒楄〃锛堝悜鍚庡吋瀹癸級
	Questions    []QuestionItem `json:"questions"`     // 璇︾粏鐨勫瓙闂鍒楄〃锛堟柊鐗堬級
	IsPermission bool           `json:"is_permission"` // 鏄惁鏄潈闄愯姹?
	Directory    string         `json:"directory"`     // 鏉冮檺璇锋眰鐨勫伐浣滅洰褰?
	CreatedAt    time.Time      `json:"created_at"`
}

// Client knows how to talk to the remote OpenCode service using the official SDK.
type Client struct {
	sdk               *opencode.Client
	endpoint          string
	apiKey            string
	basicAuthHeader   string
	httpClient        *http.Client
	eventHandlers     []EventHandler
	eventListenerMu   sync.RWMutex
	sessionHandlers   sync.Map     // map[sessionID]EventHandler for fast lookup
	activeHandlers    sync.Map     // map[sessionID]*StreamingSessionHandler for todo/diff access
	messageToSession  sync.Map     // map[messageID]sessionID for events with only messageID
	messageRoles      sync.Map     // map[messageID]role ("user"/"assistant") for filtering user message delta events
	sessionMu         sync.RWMutex // 鐢ㄤ簬淇濇姢 session 鐩稿叧鎿嶄綔
	sessions          sync.Map     // map[threadID]sessionID
	sessionLocks      sync.Map     // map[threadID]*sync.Mutex for preventing concurrent session operations
	sessionsMu        sync.RWMutex // 淇濇姢 sessions 鐨勮鍐?
	messageCount      sync.Map     // map[sessionID]int tracks messages per session
	tokenCount        sync.Map     // map[sessionID]int tracks estimated tokens per session
	sessionSummary    sync.Map     // map[sessionID]string stores session summaries
	modelConfig       sync.Map     // map[sessionID]*ModelConfig caches model config per session
	sessionModel      sync.Map     // map[sessionID]*ModelConfig tracks latest provider/model seen in assistant replies
	sessionOverride   sync.Map     // map[sessionID]*ModelConfig stores user-selected model via /model
	requestCache      sync.Map     // map[requestHash]*RequestRecord 璇锋眰鍘婚噸缂撳瓨
	runningSessions   sync.Map     // map[sessionID]bool 璺熻釜姝ｅ湪杩愯鐨剆ession
	pendingQuestions  sync.Map     // map[questionID]*Question 寰呭洖绛旂殑闂
	modelCatalogMu    sync.RWMutex
	modelCatalog      map[string]*ModelCapability // key: providerID/modelID
	defaultModelMu    sync.RWMutex
	defaultModel      *ModelConfig
	directory         string
	timeout           time.Duration // 榛樿瓒呮椂鏃堕棿
	retryConfig       RetryConfig   // 閲嶈瘯閰嶇疆
	debugMediaRouting bool          // 鏄惁鍚敤澶氭ā鎬佽矾鐢辫皟璇曟棩蹇?
	enableSkillHint   bool          // 鏄惁鍦ㄦ秷鎭腑娣诲姞skill鎻愮ず
	skillHintCache    []string      // 缂撳瓨鐨勫彲鐢╯kill鍒楄〃
	skillCacheMu      sync.RWMutex
	memoryEnabled     bool
	memoryDir         string
	memoryMaxChars    int
	memoryMaxFacts    int
	memoryInjectFacts int
	memoryMu          sync.Mutex
	lastHealthCheck   time.Time    // 鏈€鍚庝竴娆″仴搴锋鏌ユ椂闂?
	isHealthy         bool         // OpenCode server鏄惁鍋ュ悍
	healthCheckMu     sync.RWMutex // 淇濇姢鍋ュ悍鐘舵€?
	thinkingEnabled   atomic.Bool  // 鏄惁杈撳嚭 reasoning/thinking 鍐呭
	finalOnlyEnabled  atomic.Bool  // 鏄惁浠呭湪缁撴潫鏃跺彂閫佹渶缁堝洖澶?
	stepEnabled       atomic.Bool  // 鏄惁鏄剧ず step-start/step-finish 涓棿姝ラ
	serverUnavailable ServerUnavailableHandler
	unavailableMu     sync.Mutex
	lastUnavailableAt time.Time
	unavailableDelay  time.Duration
}

type memoryFact struct {
	Text       string    `json:"text"`
	Category   string    `json:"category"`
	Importance int       `json:"importance"`
	LastSeen   time.Time `json:"last_seen"`
	Count      int       `json:"count"`
}

type userMemoryStore struct {
	Version int          `json:"version"`
	Facts   []memoryFact `json:"facts"`
}

// MemoryFactView is a safe exported view used by adapters.
type MemoryFactView struct {
	Text       string
	Category   string
	Importance int
	LastSeen   time.Time
	Count      int
}

// Option mutates a client during construction.
type Option func(*Client)

// WithDirectory sets the working directory for sessions.
func WithDirectory(dir string) Option {
	return func(c *Client) {
		c.directory = dir
	}
}

// Directory returns the current working directory context used by OpenCode APIs.
func (c *Client) Directory() string {
	return strings.TrimSpace(c.directory)
}

// WithTimeout sets the default timeout for OpenCode operations.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithEventHandler registers a handler for incoming events.
func WithEventHandler(handler EventHandler) Option {
	return func(c *Client) {
		c.eventHandlers = append(c.eventHandlers, handler)
	}
}

// WithRetryConfig sets custom retry configuration.
func WithRetryConfig(cfg RetryConfig) Option {
	return func(c *Client) {
		c.retryConfig = cfg
	}
}

// WithSkillHint enables automatic skill hint injection.
func WithSkillHint(enable bool) Option {
	return func(c *Client) {
		c.enableSkillHint = enable
	}
}

// WithDebugMediaRouting enables detailed media routing debug logs.
func WithDebugMediaRouting(enable bool) Option {
	return func(c *Client) {
		c.debugMediaRouting = enable
	}
}

// WithServerUnavailableHandler installs a self-healing hook used when the
// OpenCode server is unreachable (for example connection refused on /event).
func WithServerUnavailableHandler(handler ServerUnavailableHandler) Option {
	return func(c *Client) {
		c.serverUnavailable = handler
	}
}

func parseEnvBool(key string) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err == nil {
		return b
	}
	switch strings.ToLower(v) {
	case "1", "y", "yes", "on", "enable", "enabled":
		return true
	default:
		return false
	}
}

// NewClient builds a Client instance using the official OpenCode SDK.
func NewClient(endpoint, apiKey string, opts ...Option) *Client {
	sdkOptions := []option.RequestOption{
		option.WithBaseURL(endpoint),
	}
	trimmedAPIKey := strings.TrimSpace(apiKey)
	serverPassword := strings.TrimSpace(os.Getenv("OPENCODE_SERVER_PASSWORD"))
	basicAuthHeader := ""
	if serverPassword != "" {
		username := strings.TrimSpace(os.Getenv("OPENCODE_SERVER_USERNAME"))
		if username == "" {
			username = "opencode"
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + serverPassword))
		basicAuthHeader = "Basic " + encoded
		sdkOptions = append(sdkOptions, option.WithHeader("Authorization", basicAuthHeader))
	} else if trimmedAPIKey != "" {
		sdkOptions = append(sdkOptions,
			option.WithHeader("Authorization", "Bearer "+trimmedAPIKey),
			option.WithHeader("X-API-Key", trimmedAPIKey),
		)
	}

	client := &Client{
		sdk:             opencode.NewClient(sdkOptions...),
		endpoint:        strings.TrimRight(endpoint, "/"),
		apiKey:          trimmedAPIKey,
		basicAuthHeader: basicAuthHeader,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		eventHandlers: make([]EventHandler, 0),
		directory:     ".",
		timeout:       1200 * time.Second, // 20鍒嗛挓瓒呮椂锛岀粰澶嶆潅浠诲姟锛堝妯″瀷寰皟銆佸ぇ瑙勬ā浠ｇ爜鐢熸垚锛夎冻澶熸椂闂?
		retryConfig: RetryConfig{
			MaxRetries:   MaxRetries,
			InitialDelay: InitialRetryDelay,
			MaxDelay:     MaxRetryDelay,
			RetryableErrors: []string{
				// 娉ㄦ剰锛氫笉鍖呮嫭 "context deadline exceeded"锛屽洜涓鸿秴鏃舵剰鍛崇潃浠诲姟闇€瑕佹洿闀挎椂闂?
				// 閲嶈瘯浼氬鑷撮噸澶嶅彂閫佽姹傚埌OpenCode
				"connection refused",
				"connection reset",
				"temporarily unavailable",
				"503",
				"502",
				"500",
			},
		},
		enableSkillHint:   false, // 榛樿绂佺敤skill鎻愮ず
		debugMediaRouting: parseEnvBool("OPENBOT_DEBUG_MEDIA_ROUTING"),
		modelCatalog:      make(map[string]*ModelCapability),
		memoryEnabled:     parseEnvBool("OPENCODE_GATEWAY_MEMORY_ENABLED"),
		memoryDir:         strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_DIR")),
		memoryMaxChars:    4000,
		memoryMaxFacts:    40,
		memoryInjectFacts: 8,
		isHealthy:         false,       // 鍒濆鐘舵€佹湭鐭?
		lastHealthCheck:   time.Time{}, // 鏈鏌ヨ繃
		unavailableDelay:  20 * time.Second,
	}
	if client.memoryDir == "" {
		client.memoryDir = filepath.Join(client.directory, ".opencode-gateway-memory")
	}
	if rawMax := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_MAX_CHARS")); rawMax != "" {
		if parsed, err := strconv.Atoi(rawMax); err == nil && parsed > 0 {
			client.memoryMaxChars = parsed
		}
	}
	if rawMaxFacts := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_MAX_FACTS")); rawMaxFacts != "" {
		if parsed, err := strconv.Atoi(rawMaxFacts); err == nil && parsed > 0 {
			client.memoryMaxFacts = parsed
		}
	}
	if rawInjectFacts := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_INJECT_FACTS")); rawInjectFacts != "" {
		if parsed, err := strconv.Atoi(rawInjectFacts); err == nil && parsed > 0 {
			client.memoryInjectFacts = parsed
		}
	}

	for _, opt := range opts {
		opt(client)
	}

	client.thinkingEnabled.Store(parseEnvBool("OPENBOT_SHOW_THINKING"))
	log.Printf("opencode: thinking output enabled=%t (env OPENBOT_SHOW_THINKING)", client.thinkingEnabled.Load())
	client.finalOnlyEnabled.Store(parseEnvBool("OPENBOT_FINAL_ONLY"))
	log.Printf("opencode: final-only output enabled=%t (env OPENBOT_FINAL_ONLY)", client.finalOnlyEnabled.Load())
	client.stepEnabled.Store(parseEnvBool("OPENBOT_SHOW_STEPS"))
	log.Printf("opencode: step output enabled=%t (env OPENBOT_SHOW_STEPS)", client.stepEnabled.Load())

	// 鍚姩鍚庡彴娓呯悊鍗忕▼
	go client.cleanupRequestCache()

	// 鍚姩鍚庤繘琛岄娆″仴搴锋鏌?
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := client.CheckHealth(ctx); err != nil {
			log.Printf("opencode: initial health check failed: %v", err)
		} else {
			log.Printf("opencode: initial health check succeeded")
			if err := client.refreshModelCatalog(ctx); err != nil {
				log.Printf("opencode: initial model catalog fetch failed: %v", err)
			} else {
				log.Printf("opencode: initial model catalog loaded")
			}
		}
	}()

	return client
}

func (c *Client) applyAuthHeaders(header http.Header) {
	if c.basicAuthHeader != "" {
		header.Set("Authorization", c.basicAuthHeader)
		header.Del("X-API-Key")
		return
	}
	if c.apiKey == "" {
		return
	}
	header.Set("Authorization", "Bearer "+c.apiKey)
	header.Set("X-API-Key", c.apiKey)
}

// Ready reports if the client has enough data to operate.
func (c *Client) Ready() bool {
	return c.sdk != nil
}

// SetThinkingEnabled toggles whether reasoning/thinking stream is emitted to adapters.
func (c *Client) SetThinkingEnabled(enabled bool) {
	c.thinkingEnabled.Store(enabled)
	log.Printf("opencode: thinking output toggled to %t", enabled)
}

// IsThinkingEnabled reports whether reasoning/thinking stream emission is enabled.
func (c *Client) IsThinkingEnabled() bool {
	return c.thinkingEnabled.Load()
}

// SetFinalOnlyEnabled toggles whether adapters should buffer final output until completion.
func (c *Client) SetFinalOnlyEnabled(enabled bool) {
	c.finalOnlyEnabled.Store(enabled)
	log.Printf("opencode: final-only output toggled to %t", enabled)
}

// IsFinalOnlyEnabled reports whether final-only output mode is enabled.
func (c *Client) IsFinalOnlyEnabled() bool {
	return c.finalOnlyEnabled.Load()
}

// SetStepEnabled toggles whether step lifecycle messages are emitted to adapters.
func (c *Client) SetStepEnabled(enabled bool) {
	c.stepEnabled.Store(enabled)
	log.Printf("opencode: step output toggled to %t", enabled)
}

// IsStepEnabled reports whether step lifecycle messages are enabled.
func (c *Client) IsStepEnabled() bool {
	return c.stepEnabled.Load()
}

// CheckHealth checks if the OpenCode server is running and accessible.
// It caches the result for 10 seconds to avoid excessive health checks.
func (c *Client) CheckHealth(ctx context.Context) error {
	// 妫€鏌ョ紦瀛樼殑鍋ュ悍鐘舵€侊紙10绉掑唴锛?
	c.healthCheckMu.RLock()
	if time.Since(c.lastHealthCheck) < 10*time.Second && c.isHealthy {
		c.healthCheckMu.RUnlock()
		return nil
	}
	c.healthCheckMu.RUnlock()

	// 鎵ц鍋ュ悍妫€鏌ワ細灏濊瘯鍒楀嚭sessions
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := c.sdk.Session.List(ctx, opencode.SessionListParams{})

	c.healthCheckMu.Lock()
	c.lastHealthCheck = time.Now()
	if err != nil {
		c.isHealthy = false
		c.healthCheckMu.Unlock()
		c.maybeHandleServerUnavailable(context.Background(), err, "health-check")
		return c.formatHealthCheckError(err)
	}
	c.isHealthy = true
	c.healthCheckMu.Unlock()

	return nil
}

func (c *Client) formatHealthCheckError(err error) error {
var apiErr *opencode.Error
if errors.As(err, &apiErr) {
switch apiErr.StatusCode {
case http.StatusUnauthorized:
return fmt.Errorf("opencode server unauthorized (401): %w; endpoint=%s", err, c.endpoint)
case http.StatusForbidden:
return fmt.Errorf("opencode server forbidden (403): %w", err)
case http.StatusNotFound:
return fmt.Errorf("opencode server endpoint not found (404): %w; endpoint=%s", err, c.endpoint)
default:
return fmt.Errorf("opencode server status %d: %w; endpoint=%s", apiErr.StatusCode, err, c.endpoint)
}
}

var urlErr *url.Error
if errors.As(err, &urlErr) {
return fmt.Errorf("opencode server unreachable: %w; endpoint=%s", err, c.endpoint)
}

if errors.Is(err, context.DeadlineExceeded) {
return fmt.Errorf("opencode server health check timeout: %w; endpoint=%s", err, c.endpoint)
}

return fmt.Errorf("opencode server unavailable: %w; endpoint=%s", err, c.endpoint)
}

// IsHealthy returns the cached health status.
func (c *Client) IsHealthy() bool {
	c.healthCheckMu.RLock()
	defer c.healthCheckMu.RUnlock()
	return c.isHealthy
}

// SendMessage forwards an adapter payload to OpenCode and returns its response.
// 娉ㄦ剰锛歄penCode鏀寔涓ょ妯″紡锛?
// 1. POST /session/:id/message - 鍚屾妯″紡锛岀瓑寰呭搷搴斿悗杩斿洖
// 2. POST /session/:id/prompt_async - 寮傛妯″紡锛岀珛鍗宠繑鍥?04锛岄€氳繃浜嬩欢娴佽幏鍙栫粨鏋?
// 瀵逛簬闀挎椂闂翠换鍔★紝搴旇浣跨敤寮傛妯″紡+浜嬩欢鐩戝惉
func (c *Client) SendMessage(ctx context.Context, payload MessagePayload) (Response, error) {
	if !c.Ready() {
		return Response{}, fmt.Errorf("opencode: client not configured")
	}

	if strings.TrimSpace(payload.Content) == "" {
		return Response{}, ErrEmptyPayload
	}
	originalContent := payload.Content

	if c.memoryEnabled {
		if memory := c.renderUserMemoryForPrompt(payload.Channel, payload.UserID); memory != "" {
			payload.Content = fmt.Sprintf("[鐢ㄦ埛闀挎湡璁板繂]\n%s\n\n[鐢ㄦ埛褰撳墠娑堟伅]\n%s", memory, originalContent)
			log.Printf("opencode: injected user memory for %s/%s (%d chars)", payload.Channel, payload.UserID, len(memory))
		}
	}

	// ========== 鍋ュ悍妫€鏌ワ細纭繚OpenCode server宸插惎鍔?==========
	if err := c.CheckHealth(ctx); err != nil {
		return Response{}, err
	}

	// ========== 璇锋眰鍘婚噸妫€鏌ワ紙浠呴槻姝㈠揩閫熼噸澶嶇偣鍑伙級==========
	requestHash := generateRequestHash(payload)
	if record, isDuplicate := c.checkAndMarkRequest(requestHash); isDuplicate {
		if !record.InFlight {
			// 宸插畬鎴愮殑璇锋眰锛岃繑鍥炵紦瀛樼殑鍝嶅簲锛堝揩閫熷搷搴旓級
			// 杩欎笉鏄湡姝ｇ殑閲嶅锛屽彧鏄紦瀛樺懡涓?
			return record.Response, nil
		}
		// 璇锋眰姝ｅ湪澶勭悊涓紙30绉掑唴鐨勫揩閫熼噸澶嶇偣鍑伙級
		log.Printf("opencode: duplicate request detected, request is still processing (in-flight)")
		return Response{}, ErrDuplicateRequest
	}

	// 纭繚璇锋眰瀹屾垚鏃舵洿鏂扮姸鎬?
	defer func() {
		// 濡傛灉鍙戠敓panic锛屾竻鐞嗚姹傜姸鎬?
		if r := recover(); r != nil {
			c.failRequest(requestHash)
			panic(r)
		}
	}()

	// 浣跨敤鐙珛鐨刢ontext鐢ㄤ簬session鍒涘缓锛岄伩鍏嶈澶栭儴context鍙栨秷褰卞搷
	sessionCtx, sessionCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer sessionCancel()

	// Get or create session with lock to prevent concurrent session creation
	threadLock := c.getThreadLock(payload.ThreadID)
	threadLock.Lock()
	sessionID := payload.SessionID

	// 馃攳 璇婃柇鏃ュ織锛氳褰?session 鏌ユ壘璇锋眰
	log.Printf("opencode: session lookup - channel=%s, userID=%s, threadID=%s, requestingSessionID=%s",
		payload.Channel, payload.UserID, payload.ThreadID, sessionID)

	if sessionID == "" && payload.ThreadID != "" {
		if sid, ok := c.sessions.Load(payload.ThreadID); ok {
			foundSessionID := sid.(string)

			// 馃攳 璇婃柇鏃ュ織锛氭鏌?session 鏄惁灞炰簬褰撳墠鐢ㄦ埛
			// 閫氳繃鏌ヨ adapter 鐨勬槧灏勬潵楠岃瘉锛堝鏋滃彲鐢級
			log.Printf("opencode: found cached session %s for threadID %s (requested by %s user %s)",
				foundSessionID[:8], payload.ThreadID, payload.Channel, payload.UserID)

			// 璀﹀憡锛氬彲鑳藉瓨鍦?session 娣风敤
			log.Printf("opencode: 鈿狅笍 WARNING - ThreadID %s is mapped to session %s, but cannot verify ownership!",
				payload.ThreadID, foundSessionID[:8])

			sessionID = foundSessionID
		} else {
			log.Printf("opencode: no cached session for threadID %s, will create new", payload.ThreadID)
		}
	}

	// Create new session if needed
	if sessionID == "" {
		// 馃攳 璇婃柇鏃ュ織锛氬垱寤烘柊 session
		log.Printf("opencode: creating new session - channel=%s, userID=%s, threadID=%s",
			payload.Channel, payload.UserID, payload.ThreadID)

		// 灏?adapter 鍜?user 淇℃伅缂栫爜鍒?Title 涓紝鏍煎紡: [adapter:userId] threadId
		sessionTitle := fmt.Sprintf("[%s:%s] %s", payload.Channel, payload.UserID, payload.ThreadID)

		session, err := c.sdk.Session.New(sessionCtx, opencode.SessionNewParams{
			Title: opencode.F(sessionTitle),
		})
		if err != nil {
			threadLock.Unlock()
			c.failRequest(requestHash)
			return Response{}, fmt.Errorf("opencode: create session: %w", err)
		}
		sessionID = session.ID
		if payload.ThreadID != "" {
			c.sessions.Store(payload.ThreadID, sessionID)
			// 馃攳 璇婃柇鏃ュ織锛氳褰?session 鏄犲皠
			log.Printf("opencode: mapped threadID %s -> sessionID %s (for %s user %s)",
				payload.ThreadID, sessionID[:8], payload.Channel, payload.UserID)
		}
		c.messageCount.Store(sessionID, 0)
		c.tokenCount.Store(sessionID, 0)

		// 鑾峰彇妯″瀷閰嶇疆
		go c.fetchAndCacheModelConfig(context.Background(), sessionID)

		log.Printf("opencode: created new session %s for thread %s", sessionID[:8], payload.ThreadID)
	} else {
		// 馃攳 璇婃柇鏃ュ織锛氬鐢ㄧ幇鏈?session
		log.Printf("opencode: reusing existing sessionID %s for %s user %s (threadID %s)",
			sessionID[:8], payload.Channel, payload.UserID, payload.ThreadID)

		// 楠岃瘉 session 鏄惁浠嶇劧鏈夋晥锛圤penCode Server 閲嶅惎鍚?session 鍙兘澶辨晥锛?
		checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, checkErr := c.GetSession(checkCtx, sessionID)
		checkCancel()
		if checkErr != nil {
			log.Printf("opencode: 鈿狅笍 session %s is stale (err: %v), creating new session",
				sessionID[:8], checkErr)

			// 娓呴櫎鏃ф槧灏?
			if payload.ThreadID != "" {
				c.sessions.Delete(payload.ThreadID)
			}
			c.messageCount.Delete(sessionID)
			c.tokenCount.Delete(sessionID)
			c.modelConfig.Delete(sessionID)

			// 鍒涘缓鏂?session
			sessionTitle := fmt.Sprintf("[%s:%s] %s", payload.Channel, payload.UserID, payload.ThreadID)
			newSession, err := c.sdk.Session.New(sessionCtx, opencode.SessionNewParams{
				Title: opencode.F(sessionTitle),
			})
			if err != nil {
				threadLock.Unlock()
				c.failRequest(requestHash)
				return Response{}, fmt.Errorf("opencode: create replacement session: %w", err)
			}
			sessionID = newSession.ID
			if payload.ThreadID != "" {
				c.sessions.Store(payload.ThreadID, sessionID)
				log.Printf("opencode: 馃攧 remapped threadID %s -> new sessionID %s (replaced stale session)",
					payload.ThreadID, sessionID[:8])
			}
			c.messageCount.Store(sessionID, 0)
			c.tokenCount.Store(sessionID, 0)
			go c.fetchAndCacheModelConfig(context.Background(), sessionID)
			// 璺宠繃鍚庣画鐨?token 妫€鏌ワ紝鐩存帴浣跨敤鏂?session
			threadLock.Unlock()
			goto sendMessage
		}

		// 妫€鏌ユ槸鍚﹂渶瑕佹€荤粨鎴栧垱寤烘柊session
		msgCount := c.loadCounter(&c.messageCount, sessionID)
		currentTokens := c.loadCounter(&c.tokenCount, sessionID)

		// 浼扮畻褰撳墠娑堟伅鐨則oken鏁?
		estimatedMsgTokens := estimateTokens(payload.Content)
		projectedTokens := currentTokens + estimatedMsgTokens

		// 鑾峰彇妯″瀷涓婁笅鏂囬暱搴?
		maxContextTokens := c.getMaxContextLength(sessionID)
		contextUsage := float64(projectedTokens) / float64(maxContextTokens)

		log.Printf("opencode: session %s - messages: %d, tokens: %d/%d (%.1f%%), estimated msg tokens: %d",
			sessionID[:8], msgCount, currentTokens, maxContextTokens, contextUsage*100, estimatedMsgTokens)

		// 濡傛灉涓婁笅鏂囦娇鐢ㄧ巼瓒呰繃闃堝€硷紝鍒涘缓鏂皊ession
		if contextUsage >= ContextUsageThreshold {
			log.Printf("opencode: session %s context usage %.1f%% >= threshold %.1f%%, creating new session",
				sessionID[:8], contextUsage*100, ContextUsageThreshold*100)

			// 鎬荤粨鏃ession
			if err := c.SummarizeSession(ctx, sessionID); err != nil {
				log.Printf("opencode: failed to summarize session %s: %v", sessionID, err)
			}

			// 鑾峰彇鎬荤粨鍐呭
			summary := ""
			if sum, ok := c.sessionSummary.Load(sessionID); ok {
				summary = sum.(string)
			}

			// 鍒涘缓鏂皊ession锛屾爣棰樺寘鍚巻鍙蹭俊鎭?
			title := fmt.Sprintf("%s-%s-next", payload.Channel, payload.UserID)
			if summary != "" {
				title = fmt.Sprintf("%s-%s (涔嬪墠璁ㄨ: %s)", payload.Channel, payload.UserID, truncateString(summary, 50))
			}

			newSession, err := c.sdk.Session.New(ctx, opencode.SessionNewParams{
				Title: opencode.F(title),
			})
			if err != nil {
				log.Printf("opencode: failed to create new session: %v, continuing with current", err)
			} else {
				sessionID = newSession.ID
				if payload.ThreadID != "" {
					c.sessions.Store(payload.ThreadID, sessionID)
				}
				c.messageCount.Store(sessionID, 0)
				c.tokenCount.Store(sessionID, 0)

				// 鑾峰彇鏂皊ession鐨勬ā鍨嬮厤缃?
				go c.fetchAndCacheModelConfig(context.Background(), sessionID)

				// 濡傛灉鏈夋€荤粨锛屽皢鎬荤粨浣滀负绯荤粺娑堟伅娣诲姞鍒版柊session鐨勪笂涓嬫枃
				if summary != "" {
					contextMsg := fmt.Sprintf("[涓婁竴杞璇濇€荤粨]: %s\n\n[鐢ㄦ埛鏂版秷鎭痌: %s", summary, payload.Content)
					payload.Content = contextMsg
					estimatedMsgTokens = estimateTokens(contextMsg) // 閲嶆柊浼扮畻
					log.Printf("opencode: created new session %s with context from previous session", sessionID)
				} else {
					log.Printf("opencode: created new session %s for thread %s", sessionID, payload.ThreadID)
				}
			}
		} else if contextUsage >= SummaryThreshold && msgCount%5 == 0 {
			// 鍦ㄨ揪鍒版€荤粨闃堝€煎悗锛屾瘡5鏉℃秷鎭皾璇曟€荤粨涓€娆★紙鍚庡彴寮傛锛?
			log.Printf("opencode: session %s context usage %.1f%% >= summary threshold %.1f%%, scheduling background summary",
				sessionID[:8], contextUsage*100, SummaryThreshold*100)
			go func(sid string) {
				sumCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := c.SummarizeSession(sumCtx, sid); err != nil {
					log.Printf("opencode: background summarization failed for session %s: %v", sid, err)
				}
			}(sessionID)
		}
	}
	threadLock.Unlock()

sendMessage:
	// ========== 澧炲己娑堟伅鍐呭 ==========
	// 娣诲姞skill鎻愮ず锛堜粎鍦╯ession寮€濮嬫椂锛?
	enhancedContent := c.enhanceContentWithSkillHint(payload.Content, sessionID)
	effectiveContent := enhancedContent

	// ========== 澶氭ā鎬佸吋瀹瑰鐞?==========
	// 鑻ュ綋鍓嶄細璇濇ā鍨嬩笉鏀寔鍥剧墖/瑙嗛锛屽垯浣跨敤鏀寔妯″瀷鍏堣瘑鍒獟浣擄紝鍐嶅皢璇嗗埆缁撴灉杞负鏂囨湰鍙戠粰褰撳墠浼氳瘽妯″瀷銆?
	if processed, err := c.preprocessAttachmentsForSession(ctx, sessionID, &payload, &effectiveContent); err != nil {
		log.Printf("opencode: media preprocessing failed for session %s: %v", sessionID[:8], err)
		c.failRequest(requestHash)
		return Response{}, fmt.Errorf("opencode: media preprocessing: %w", err)
	} else if processed {
		log.Printf("opencode: media preprocessing applied for session %s", sessionID[:8])
	}

	mainModelOverride := c.getSessionModelOverride(sessionID)
	if mainModelOverride != nil {
		log.Printf("opencode: request route - session=%s mainModel=%s/%s attachments=%d",
			sessionID[:8], mainModelOverride.ProviderID.Value, mainModelOverride.ModelID.Value, len(payload.Attachments))
	} else {
		log.Printf("opencode: request route - session=%s mainModel=<default/session> attachments=%d",
			sessionID[:8], len(payload.Attachments))
	}

	// Build message parts
	parts := []opencode.SessionPromptParamsPartUnion{}

	// Add agent part if specified
	// Note: OpenCode鏀寔澶氱妯″紡锛?
	// - chat: 鏅€氬璇濇ā寮忥紝鏃犻渶纭
	// - plan: 瑙勫垝妯″紡锛屼細鐢熸垚璁″垝
	// - build: 鏋勫缓妯″紡锛岄渶瑕佺敤鎴风‘璁ゆ墠鎵ц锛堝彲鑳藉鑷寸瓑寰咃級
	if payload.Agent != "" {
		parts = append(parts, opencode.AgentPartInputParam{
			Name: opencode.F(payload.Agent),
			Type: opencode.F(opencode.AgentPartInputTypeAgent),
		})
		log.Printf("opencode: using agent '%s' for session %s", payload.Agent, sessionID[:8])
	}

	// Add text content (浣跨敤澧炲己鍚庣殑鍐呭)
	parts = append(parts, opencode.TextPartInputParam{
		Text: opencode.F(effectiveContent),
		Type: opencode.F(opencode.TextPartInputTypeText),
	})

	// Add file attachments (images, audio, video as data URIs)
	for _, att := range payload.Attachments {
		if att.URL == "" || att.Mime == "" {
			continue
		}
		fp := opencode.FilePartInputParam{
			Type: opencode.F(opencode.FilePartInputTypeFile),
			Mime: opencode.F(att.Mime),
			URL:  opencode.F(att.URL),
		}
		if att.Filename != "" {
			fp.Filename = opencode.F(att.Filename)
		}
		parts = append(parts, fp)
		log.Printf("opencode: added %s attachment (%s, %d bytes data URI) to session %s",
			att.Mime, att.Filename, len(att.URL), sessionID[:8])
	}

	// 娴佸紡妯″紡涓嬫敼鐢ㄥ紓姝?prompt_async锛岄伩鍏嶉暱浠诲姟瀵艰嚧 context deadline
	if payload.Streaming {
		c.runningSessions.Store(sessionID, true)
		if err := c.sendPromptAsync(ctx, sessionID, parts, mainModelOverride); err != nil {
			c.runningSessions.Delete(sessionID)
			c.failRequest(requestHash)
			return Response{}, fmt.Errorf("opencode: prompt_async: %w", err)
		}

		// 浠呯粺璁＄敤鎴锋秷鎭湰韬殑tokens锛屽洖澶嶅湪浜嬩欢娴佷腑鑾峰彇
		c.incrementCounter(&c.messageCount, sessionID, 1)
		estimatedMsgTokens := estimateTokens(effectiveContent)
		c.incrementCounter(&c.tokenCount, sessionID, estimatedMsgTokens)

		response := Response{
			Reply:     "",
			SessionID: sessionID,
			MessageID: "",
			Trace:     sessionID,
		}

		c.completeRequest(requestHash, response)
		return response, nil
	}

	// ========== 浣跨敤閲嶈瘯鏈哄埗鍙戦€佹秷鎭?==========
	// 鏍囪session涓鸿繍琛岀姸鎬?
	c.runningSessions.Store(sessionID, true)

	result, err := c.sendPromptWithRetry(ctx, sessionID, parts, mainModelOverride)

	// 娓呴櫎杩愯鐘舵€?
	c.runningSessions.Delete(sessionID)

	if err != nil {
		c.failRequest(requestHash)
		return Response{}, fmt.Errorf("opencode: send prompt: %w", err)
	}

	// Extract reply from assistant message
	reply := extractReplyFromMessage(result)

	// Increment message count and token count for this session
	c.incrementCounter(&c.messageCount, sessionID, 1)

	// 鏇存柊token璁℃暟锛堜及绠楃敤鎴锋秷鎭?+ AI鍥炲锛?
	estimatedMsgTokens := estimateTokens(effectiveContent)
	estimatedReplyTokens := estimateTokens(reply)
	c.incrementCounter(&c.tokenCount, sessionID, estimatedMsgTokens+estimatedReplyTokens)

	// 缂撳瓨鏈瀹為檯浣跨敤鐨勬ā鍨嬩俊鎭紙鑻DK杩斿洖锛?
	c.updateSessionModel(sessionID, result.Info.ProviderID, result.Info.ModelID)

	response := Response{
		Reply:     reply,
		SessionID: sessionID,
		MessageID: result.Info.ID,
		Trace:     sessionID,
	}

	if c.memoryEnabled {
		c.updateUserMemory(payload.Channel, payload.UserID, originalContent, reply)
	}

	// ========== 缂撳瓨鎴愬姛鍝嶅簲鐢ㄤ簬鍘婚噸 ==========
	c.completeRequest(requestHash, response)

	return response, nil
}

func (c *Client) memoryFilePath(channel, userID string) string {
	channel = sanitizeMemoryKey(channel)
	userID = sanitizeMemoryKey(userID)
	if channel == "" {
		channel = "unknown"
	}
	if userID == "" {
		userID = "unknown"
	}
	return filepath.Join(c.memoryDir, channel+"__"+userID+".json")
}

func sanitizeMemoryKey(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range input {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

func (c *Client) loadUserMemoryStore(channel, userID string) userMemoryStore {
	path := c.memoryFilePath(channel, userID)
	b, err := os.ReadFile(path)
	if err != nil {
		return userMemoryStore{Version: 1, Facts: []memoryFact{}}
	}
	raw := strings.TrimSpace(string(b))
	if raw == "" {
		return userMemoryStore{Version: 1, Facts: []memoryFact{}}
	}

	var store userMemoryStore
	if err := json.Unmarshal([]byte(raw), &store); err == nil {
		if store.Version == 0 {
			store.Version = 1
		}
		return store
	}

	// Backward compatibility: old plain text memory file.
	return userMemoryStore{
		Version: 1,
		Facts: []memoryFact{{
			Text:       raw,
			Category:   "conversation",
			Importance: 1,
			LastSeen:   time.Now(),
			Count:      1,
		}},
	}
}

func (c *Client) renderUserMemoryForPrompt(channel, userID string) string {
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return ""
	}

	facts := append([]memoryFact(nil), store.Facts...)
	sort.Slice(facts, func(i, j int) bool {
		if facts[i].Importance != facts[j].Importance {
			return facts[i].Importance > facts[j].Importance
		}
		if !facts[i].LastSeen.Equal(facts[j].LastSeen) {
			return facts[i].LastSeen.After(facts[j].LastSeen)
		}
		return facts[i].Count > facts[j].Count
	})

	limit := c.memoryInjectFacts
	if limit <= 0 {
		limit = 8
	}
	if limit > len(facts) {
		limit = len(facts)
	}

	items := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		items = append(items, fmt.Sprintf("- [%s] %s", facts[i].Category, facts[i].Text))
	}
	prompt := strings.Join(items, "\n")
	runes := []rune(prompt)
	if len(runes) > c.memoryMaxChars {
		prompt = string(runes[:c.memoryMaxChars])
	}
	return prompt
}

func (c *Client) updateUserMemory(channel, userID, userMessage, assistantReply string) {
	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()

	if err := os.MkdirAll(c.memoryDir, 0o755); err != nil {
		log.Printf("opencode: create memory dir failed: %v", err)
		return
	}

	path := c.memoryFilePath(channel, userID)
	store := c.loadUserMemoryStore(channel, userID)
	candidates := extractMemoryCandidates(userMessage, assistantReply)
	if len(candidates) == 0 {
		return
	}

	for _, cand := range candidates {
		merged := false
		for i := range store.Facts {
			if normalizeMemoryText(store.Facts[i].Text) == normalizeMemoryText(cand.Text) {
				store.Facts[i].LastSeen = time.Now()
				store.Facts[i].Count++
				if cand.Importance > store.Facts[i].Importance {
					store.Facts[i].Importance = cand.Importance
				}
				merged = true
				break
			}
		}
		if !merged {
			cand.LastSeen = time.Now()
			cand.Count = 1
			store.Facts = append(store.Facts, cand)
		}
	}

	sort.Slice(store.Facts, func(i, j int) bool {
		if store.Facts[i].Importance != store.Facts[j].Importance {
			return store.Facts[i].Importance > store.Facts[j].Importance
		}
		if !store.Facts[i].LastSeen.Equal(store.Facts[j].LastSeen) {
			return store.Facts[i].LastSeen.After(store.Facts[j].LastSeen)
		}
		return store.Facts[i].Count > store.Facts[j].Count
	})

	if c.memoryMaxFacts > 0 && len(store.Facts) > c.memoryMaxFacts {
		store.Facts = store.Facts[:c.memoryMaxFacts]
	}

	encoded, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		log.Printf("opencode: marshal memory failed: %v", err)
		return
	}

	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		log.Printf("opencode: write memory failed: %v", err)
	}
}

// ListUserMemory returns top memory facts sorted by priority.
func (c *Client) ListUserMemory(channel, userID string, limit int) []MemoryFactView {
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return nil
	}

	facts := c.sortedMemoryFacts(store.Facts)

	if limit <= 0 || limit > len(facts) {
		limit = len(facts)
	}
	out := make([]MemoryFactView, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, MemoryFactView{
			Text:       facts[i].Text,
			Category:   facts[i].Category,
			Importance: facts[i].Importance,
			LastSeen:   facts[i].LastSeen,
			Count:      facts[i].Count,
		})
	}
	return out
}

// ExportUserMemory returns base64-encoded JSON snapshot for backup/migration.
func (c *Client) ExportUserMemory(channel, userID string) (string, error) {
	store := c.loadUserMemoryStore(channel, userID)
	encoded, err := json.Marshal(store)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(encoded), nil
}

// ImportUserMemory imports a base64 JSON snapshot and replaces existing memory.
func (c *Client) ImportUserMemory(channel, userID, payload string) (int, error) {
	store, err := parseImportMemoryStore(payload)
	if err != nil {
		return 0, err
	}

	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	if err := os.MkdirAll(c.memoryDir, 0o755); err != nil {
		return 0, err
	}
	if err := c.saveMemoryStore(channel, userID, store); err != nil {
		return 0, err
	}
	return len(store.Facts), nil
}

// MergeImportUserMemory imports snapshot and merges with existing facts (dedupe + priority keep).
func (c *Client) MergeImportUserMemory(channel, userID, payload string) (int, error) {
	incoming, err := parseImportMemoryStore(payload)
	if err != nil {
		return 0, err
	}

	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	if err := os.MkdirAll(c.memoryDir, 0o755); err != nil {
		return 0, err
	}

	existing := c.loadUserMemoryStore(channel, userID)
	merged := map[string]memoryFact{}
	for _, f := range existing.Facts {
		key := normalizeMemoryText(f.Text)
		merged[key] = f
	}
	for _, f := range incoming.Facts {
		key := normalizeMemoryText(f.Text)
		if cur, ok := merged[key]; ok {
			if f.Importance > cur.Importance {
				cur.Importance = f.Importance
				cur.Category = f.Category
			}
			if f.LastSeen.After(cur.LastSeen) {
				cur.LastSeen = f.LastSeen
			}
			cur.Count += f.Count
			merged[key] = cur
			continue
		}
		merged[key] = f
	}

	facts := make([]memoryFact, 0, len(merged))
	for _, f := range merged {
		facts = append(facts, f)
	}
	existing.Facts = facts
	if err := c.saveMemoryStore(channel, userID, existing); err != nil {
		return 0, err
	}
	return len(existing.Facts), nil
}

// ClearUserMemory deletes persisted memory for the user.
func (c *Client) ClearUserMemory(channel, userID string) error {
	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	path := c.memoryFilePath(channel, userID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// PinUserMemory stores a high-priority explicit memory fact.
func (c *Client) PinUserMemory(channel, userID, text, category string) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return fmt.Errorf("memory text is empty")
	}
	category = normalizeMemoryCategory(category)

	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	if err := os.MkdirAll(c.memoryDir, 0o755); err != nil {
		return err
	}

	store := c.loadUserMemoryStore(channel, userID)
	norm := normalizeMemoryText(text)
	for i := range store.Facts {
		if normalizeMemoryText(store.Facts[i].Text) == norm {
			store.Facts[i].Category = category
			store.Facts[i].Importance = 5
			store.Facts[i].LastSeen = time.Now()
			store.Facts[i].Count++
			return c.saveMemoryStore(channel, userID, store)
		}
	}

	store.Facts = append(store.Facts, memoryFact{
		Text:       text,
		Category:   category,
		Importance: 5,
		LastSeen:   time.Now(),
		Count:      1,
	})
	return c.saveMemoryStore(channel, userID, store)
}

// UnpinUserMemory removes memory facts containing the keyword.
func (c *Client) UnpinUserMemory(channel, userID, keyword string) (int, error) {
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return 0, fmt.Errorf("memory keyword is empty")
	}

	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return 0, nil
	}

	norm := normalizeMemoryText(keyword)
	filtered := make([]memoryFact, 0, len(store.Facts))
	removed := 0
	for _, f := range store.Facts {
		if strings.Contains(normalizeMemoryText(f.Text), norm) {
			removed++
			continue
		}
		filtered = append(filtered, f)
	}

	if removed == 0 {
		return 0, nil
	}
	store.Facts = filtered
	if err := c.saveMemoryStore(channel, userID, store); err != nil {
		return 0, err
	}
	return removed, nil
}

// RemoveUserMemoryByRank removes an entry by its display rank (1-based from /memory show order).
func (c *Client) RemoveUserMemoryByRank(channel, userID string, rank int) (bool, error) {
	if rank <= 0 {
		return false, fmt.Errorf("rank must be >= 1")
	}

	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return false, nil
	}

	sorted := c.sortedMemoryFacts(store.Facts)
	if rank > len(sorted) {
		return false, nil
	}
	target := normalizeMemoryText(sorted[rank-1].Text)

	filtered := make([]memoryFact, 0, len(store.Facts))
	removed := false
	for _, f := range store.Facts {
		if !removed && normalizeMemoryText(f.Text) == target {
			removed = true
			continue
		}
		filtered = append(filtered, f)
	}
	if !removed {
		return false, nil
	}
	store.Facts = filtered
	if err := c.saveMemoryStore(channel, userID, store); err != nil {
		return false, err
	}
	return true, nil
}

// CompactUserMemory deduplicates and trims lower-value memory entries.
func (c *Client) CompactUserMemory(channel, userID string) (int, error) {
	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return 0, nil
	}

	merged := map[string]memoryFact{}
	for _, f := range store.Facts {
		key := normalizeMemoryText(f.Text)
		existing, ok := merged[key]
		if !ok {
			merged[key] = f
			continue
		}
		if f.Importance > existing.Importance {
			existing.Importance = f.Importance
			existing.Category = f.Category
		}
		if f.LastSeen.After(existing.LastSeen) {
			existing.LastSeen = f.LastSeen
		}
		existing.Count += f.Count
		merged[key] = existing
	}

	facts := make([]memoryFact, 0, len(merged))
	for _, v := range merged {
		facts = append(facts, v)
	}

	sort.Slice(facts, func(i, j int) bool {
		if facts[i].Importance != facts[j].Importance {
			return facts[i].Importance > facts[j].Importance
		}
		if !facts[i].LastSeen.Equal(facts[j].LastSeen) {
			return facts[i].LastSeen.After(facts[j].LastSeen)
		}
		return facts[i].Count > facts[j].Count
	})

	if c.memoryMaxFacts > 0 && len(facts) > c.memoryMaxFacts {
		facts = facts[:c.memoryMaxFacts]
	}
	removed := len(store.Facts) - len(facts)
	store.Facts = facts
	if err := c.saveMemoryStore(channel, userID, store); err != nil {
		return 0, err
	}
	return removed, nil
}

func (c *Client) saveMemoryStore(channel, userID string, store userMemoryStore) error {
	path := c.memoryFilePath(channel, userID)
	sort.Slice(store.Facts, func(i, j int) bool {
		if store.Facts[i].Importance != store.Facts[j].Importance {
			return store.Facts[i].Importance > store.Facts[j].Importance
		}
		if !store.Facts[i].LastSeen.Equal(store.Facts[j].LastSeen) {
			return store.Facts[i].LastSeen.After(store.Facts[j].LastSeen)
		}
		return store.Facts[i].Count > store.Facts[j].Count
	})
	if c.memoryMaxFacts > 0 && len(store.Facts) > c.memoryMaxFacts {
		store.Facts = store.Facts[:c.memoryMaxFacts]
	}
	encoded, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0o644)
}

func (c *Client) sortedMemoryFacts(facts []memoryFact) []memoryFact {
	out := append([]memoryFact(nil), facts...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Importance != out[j].Importance {
			return out[i].Importance > out[j].Importance
		}
		if !out[i].LastSeen.Equal(out[j].LastSeen) {
			return out[i].LastSeen.After(out[j].LastSeen)
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func normalizeMemoryText(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func parseImportMemoryStore(payload string) (userMemoryStore, error) {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return userMemoryStore{}, fmt.Errorf("memory import payload is empty")
	}

	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		// Allow raw JSON payload as fallback.
		raw = []byte(payload)
	}

	var store userMemoryStore
	if err := json.Unmarshal(raw, &store); err != nil {
		return userMemoryStore{}, fmt.Errorf("invalid memory payload: %w", err)
	}
	if store.Version == 0 {
		store.Version = 1
	}

	now := time.Now()
	for i := range store.Facts {
		store.Facts[i].Category = normalizeMemoryCategory(store.Facts[i].Category)
		if store.Facts[i].Importance <= 0 {
			store.Facts[i].Importance = 1
		}
		if store.Facts[i].LastSeen.IsZero() {
			store.Facts[i].LastSeen = now
		}
		if store.Facts[i].Count <= 0 {
			store.Facts[i].Count = 1
		}
		store.Facts[i].Text = strings.TrimSpace(store.Facts[i].Text)
	}
	return store, nil
}

func normalizeMemoryCategory(category string) string {
	v := strings.ToLower(strings.TrimSpace(category))
	switch v {
	case "profile", "preference", "project", "environment", "model", "conversation":
		return v
	default:
		return "preference"
	}
}

func extractMemoryCandidates(userMessage, assistantReply string) []memoryFact {
	_ = assistantReply
	msg := strings.TrimSpace(userMessage)
	if msg == "" {
		return nil
	}

	separators := []string{"\\n", "。", ".", "；", ";", "!", "！", "?", "？"}
	parts := []string{msg}
	for _, sep := range separators {
		next := make([]string, 0, len(parts))
		for _, p := range parts {
			next = append(next, strings.Split(p, sep)...)
		}
		parts = next
	}

	facts := make([]memoryFact, 0)
	for _, p := range parts {
		text := strings.TrimSpace(p)
		if text == "" {
			continue
		}
		runes := []rune(text)
		if len(runes) < 4 || len(runes) > 140 {
			continue
		}

		cat, importance := classifyMemoryFact(text)
		if importance <= 0 {
			continue
		}
		facts = append(facts, memoryFact{
			Text:       text,
			Category:   cat,
			Importance: importance,
		})
	}

	if len(facts) > 12 {
		facts = facts[:12]
	}
	return facts
}

func classifyMemoryFact(text string) (string, int) {
	lower := strings.ToLower(strings.TrimSpace(text))

	if strings.HasPrefix(lower, "/model ") {
		return "model", 5
	}
	if strings.HasPrefix(lower, "/provider ") {
		return "environment", 5
	}
	if strings.Contains(lower, "璁颁綇") || strings.Contains(lower, "浠ュ悗") || strings.Contains(lower, "璇风敤") || strings.Contains(lower, "蹇呴』") || strings.Contains(lower, "涓嶈") {
		return "preference", 4
	}
	if strings.Contains(lower, "鎴戝彨") || strings.Contains(lower, "鍙垜") || strings.Contains(lower, "鎴戠殑") || strings.Contains(lower, "鎴戞槸") {
		return "profile", 3
	}
	if strings.Contains(lower, "椤圭洰") || strings.Contains(lower, "浠撳簱") || strings.Contains(lower, "鏈嶅姟") || strings.Contains(lower, "绔彛") || strings.Contains(lower, "鐜") {
		return "project", 3
	}
	if strings.Contains(lower, "鍠滄") || strings.Contains(lower, "涓嶅枩娆") || strings.Contains(lower, "鍋忓ソ") || strings.Contains(lower, "涔犳儻") {
		return "preference", 3
	}

	return "conversation", 1
}

// GetSession retrieves session details.
func (c *Client) GetSession(ctx context.Context, sessionID string) (*opencode.Session, error) {
	return c.sdk.Session.Get(ctx, sessionID, opencode.SessionGetParams{})
}

// GetSessionStatus retrieves the status of a session.
// 鏍规嵁OpenCode鏂囨。锛孏ET /session/status 杩斿洖鎵€鏈塻ession鐨勭姸鎬?
// 鐘舵€佸寘鎷細idle, running, error绛?
func (c *Client) GetSessionStatus(ctx context.Context, sessionID string) (string, error) {
	// TODO: SDK鍙兘闇€瑕佹坊鍔燬essionStatus鏂规硶
	// 鐩墠鍙互閫氳繃GetSession鏉ヨ幏鍙栫姸鎬?
	_, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return "", err
	}
	// 鏍规嵁session瀵硅薄鎺ㄦ柇鐘舵€?
	return "unknown", nil
}

// ListAgents retrieves all available agents/skills.
func (c *Client) ListAgents(ctx context.Context) ([]opencode.Agent, error) {
	result, err := c.sdk.Agent.List(ctx, opencode.AgentListParams{
		Directory: opencode.F(c.directory),
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []opencode.Agent{}, nil
	}
	return *result, nil
}

// ExecuteCommand executes a command in the session context.
// This can be used to directly invoke skill scripts like websearch.py
func (c *Client) ExecuteCommand(ctx context.Context, sessionID, command string) (*opencode.SessionCommandResponse, error) {
	return c.sdk.Session.Command(ctx, sessionID, opencode.SessionCommandParams{
		Command:   opencode.F(command),
		Directory: opencode.F(c.directory),
	})
}

// ExecuteShell executes a shell command in the session context.
func (c *Client) ExecuteShell(ctx context.Context, sessionID, command string) (*opencode.AssistantMessage, error) {
	return c.sdk.Session.Shell(ctx, sessionID, opencode.SessionShellParams{
		Command:   opencode.F(command),
		Directory: opencode.F(c.directory),
	})
}

// ListSessions retrieves all sessions.
func (c *Client) ListSessions(ctx context.Context) ([]opencode.Session, error) {
	result, err := c.sdk.Session.List(ctx, opencode.SessionListParams{})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []opencode.Session{}, nil
	}
	return *result, nil
}

// AbortSession aborts a running session.
// 鏍规嵁OpenCode鏂囨。锛孭OST /session/:id/abort 鍙互涓姝ｅ湪杩愯鐨剆ession
func (c *Client) AbortSession(ctx context.Context, sessionID string) error {
	log.Printf("opencode: aborting session %s", sessionID[:8])
	_, err := c.sdk.Session.Abort(ctx, sessionID, opencode.SessionAbortParams{})
	if err != nil {
		return fmt.Errorf("opencode: abort session: %w", err)
	}
	c.runningSessions.Delete(sessionID)
	return nil
}

// ForkSession forks an existing session, creating a new independent session
// branching from the forked session's history. Uses the HTTP API directly
// since the Go SDK may not expose this endpoint.
func (c *Client) ForkSession(ctx context.Context, sessionID string) (string, error) {
	if c.endpoint == "" {
		return "", fmt.Errorf("opencode: fork session unavailable: missing endpoint")
	}
	log.Printf("opencode: forking session %s", sessionID[:min(8, len(sessionID))])

	forkURL := fmt.Sprintf("%s/session/%s/fork", strings.TrimRight(c.endpoint, "/"), sessionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, forkURL, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("opencode: fork session request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("opencode: fork session do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("opencode: fork session status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("opencode: fork session decode response: %w", err)
	}
	if result.ID == "" {
		return "", fmt.Errorf("opencode: fork session returned empty ID")
	}

	log.Printf("opencode: forked session %s -> %s", sessionID[:min(8, len(sessionID))], result.ID[:min(8, len(result.ID))])
	return result.ID, nil
}

// DeleteSession deletes a session.
func (c *Client) DeleteSession(ctx context.Context, sessionID string) error {
	log.Printf("opencode: deleting session %s", sessionID[:8])
	_, err := c.sdk.Session.Delete(ctx, sessionID, opencode.SessionDeleteParams{})
	if err != nil {
		return fmt.Errorf("opencode: delete session: %w", err)
	}
	// Clean up local caches
	c.messageCount.Delete(sessionID)
	c.tokenCount.Delete(sessionID)
	c.sessionSummary.Delete(sessionID)
	c.modelConfig.Delete(sessionID)
	c.runningSessions.Delete(sessionID)
	return nil
}

// GetProviders retrieves the list of available providers.
// Uses SDK App.Providers endpoint (/config/providers).
func (c *Client) GetProviders(ctx context.Context) ([]Provider, error) {
	if !c.Ready() {
		return nil, fmt.Errorf("opencode: client not configured")
	}

	params := opencode.AppProvidersParams{}
	if c.directory != "" {
		params.Directory = opencode.F(c.directory)
	}

	result, err := c.sdk.App.Providers(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("opencode: list providers: %w", err)
	}
	if result == nil || len(result.Providers) == 0 {
		c.modelCatalogMu.Lock()
		c.modelCatalog = map[string]*ModelCapability{}
		c.modelCatalogMu.Unlock()
		return []Provider{}, nil
	}

	for _, v := range result.Default {
		parts := strings.SplitN(strings.TrimSpace(v), "/", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			c.defaultModelMu.Lock()
			c.defaultModel = &ModelConfig{ProviderID: parts[0], ModelID: parts[1], LastUpdated: time.Now()}
			c.defaultModelMu.Unlock()
			break
		}
	}

	providers := make([]Provider, 0, len(result.Providers))
	newCatalog := make(map[string]*ModelCapability)
	for _, p := range result.Providers {
		models := make([]Model, 0, len(p.Models))
		for modelKey, m := range p.Models {
			modelID := m.ID
			if modelID == "" {
				modelID = modelKey
			}

			inputModalities := make([]string, 0, len(m.Modalities.Input))
			inputMap := make(map[string]bool)
			for _, in := range m.Modalities.Input {
				s := strings.ToLower(strings.TrimSpace(string(in)))
				if s == "" {
					continue
				}
				inputModalities = append(inputModalities, s)
				inputMap[s] = true
			}

			outputModalities := make([]string, 0, len(m.Modalities.Output))
			outputMap := make(map[string]bool)
			for _, out := range m.Modalities.Output {
				s := strings.ToLower(strings.TrimSpace(string(out)))
				if s == "" {
					continue
				}
				outputModalities = append(outputModalities, s)
				outputMap[s] = true
			}

			models = append(models, Model{
				ID:               modelID,
				Name:             m.Name,
				InputModalities:  inputModalities,
				OutputModalities: outputModalities,
			})

			newCatalog[modelCatalogKey(p.ID, modelID)] = &ModelCapability{
				ProviderID:       p.ID,
				ModelID:          modelID,
				InputModalities:  inputMap,
				OutputModalities: outputMap,
			}
		}

		sort.Slice(models, func(i, j int) bool {
			return models[i].ID < models[j].ID
		})

		providers = append(providers, Provider{
			ID:     p.ID,
			Name:   p.Name,
			Models: models,
		})
	}

	c.modelCatalogMu.Lock()
	c.modelCatalog = newCatalog
	c.modelCatalogMu.Unlock()

	return providers, nil
}

// Provider represents a model provider
type Provider struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Models []Model `json:"models"`
}

// Model represents an AI model
type Model struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	InputModalities  []string `json:"input_modalities,omitempty"`
	OutputModalities []string `json:"output_modalities,omitempty"`
}

// GetCurrentProvider retrieves the current provider and model for a session.
// Note: Session may not expose provider/model info directly
func (c *Client) GetCurrentProvider(ctx context.Context, sessionID string) (*Provider, string, error) {
	_, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return nil, "", fmt.Errorf("opencode: get session: %w", err)
	}

	// SDK Session doesn't have ProviderID/ModelID fields
	// Return empty for now
	log.Printf("opencode: session %s - provider info not available in SDK", sessionID[:8])
	return nil, "", nil
}

// UpdateSessionProvider updates the provider and model for a session.
// Note: SDK may not support this directly
func (c *Client) UpdateSessionProvider(ctx context.Context, sessionID, providerID, modelID string) error {
	log.Printf("opencode: updating session %s provider to %s/%s", sessionID[:8], providerID, modelID)

	providerID = strings.TrimSpace(providerID)
	modelID = strings.TrimSpace(modelID)
	if providerID == "" || modelID == "" {
		return fmt.Errorf("opencode: provider/model cannot be empty")
	}

	override := &ModelConfig{
		ProviderID:  providerID,
		ModelID:     modelID,
		LastUpdated: time.Now(),
	}
	c.sessionOverride.Store(sessionID, override)
	c.sessionModel.Store(sessionID, override)
	log.Printf("opencode: session %s model override set to %s/%s", sessionID[:8], providerID, modelID)

	// SDK SessionUpdateParams may not have ProviderID/ModelID fields
	// Try using Update with available params
	_, err := c.sdk.Session.Update(ctx, sessionID, opencode.SessionUpdateParams{
		// Use available fields only
	})
	if err != nil {
		log.Printf("opencode: session update API ignored (local override remains active): %v", err)
	}

	// Invalidate cached model config
	c.modelConfig.Delete(sessionID)
	// Fetch new config
	go c.fetchAndCacheModelConfig(context.Background(), sessionID)

	log.Printf("opencode: note - provider/model update may not be fully supported by SDK, using gateway override")
	return nil
}

func (c *Client) getSessionModelOverride(sessionID string) *opencode.SessionPromptParamsModel {
	if strings.TrimSpace(sessionID) == "" {
		return nil
	}
	if v, ok := c.sessionOverride.Load(sessionID); ok {
		cfg := v.(*ModelConfig)
		if strings.TrimSpace(cfg.ProviderID) != "" && strings.TrimSpace(cfg.ModelID) != "" {
			return &opencode.SessionPromptParamsModel{
				ProviderID: opencode.F(cfg.ProviderID),
				ModelID:    opencode.F(cfg.ModelID),
			}
		}
	}
	return nil
}

// IsSessionRunning checks if a session is currently running
func (c *Client) IsSessionRunning(sessionID string) bool {
	val, ok := c.runningSessions.Load(sessionID)
	if !ok {
		return false
	}
	return val.(bool)
}

// StartEventListener begins listening for OpenCode events via SSE.
// 鏀寔鑷姩閲嶈繛锛氬綋 OpenCode Server 鏂紑鎴栭噸鍚悗锛屼細鑷姩閲嶆柊杩炴帴浜嬩欢娴併€?
func (c *Client) StartEventListener(ctx context.Context) error {
	log.Printf("opencode: starting event listener...")

	go c.eventListenerLoop(ctx)

	log.Printf("opencode: event listener started successfully")
	return nil
}

// eventListenerLoop 浜嬩欢鐩戝惉涓诲惊鐜紝鏀寔鑷姩閲嶈繛
func (c *Client) eventListenerLoop(ctx context.Context) {
	reconnectDelay := 2 * time.Second
	maxReconnectDelay := 60 * time.Second
	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			log.Printf("opencode: event listener context cancelled, stopping")
			return
		default:
		}

		if consecutiveFailures > 0 {
			// 鎸囨暟閫€閬块噸杩?
			delay := reconnectDelay * time.Duration(1<<uint(min(consecutiveFailures-1, 5)))
			if delay > maxReconnectDelay {
				delay = maxReconnectDelay
			}
			log.Printf("opencode: 馃攧 reconnecting event listener in %v (attempt #%d)...", delay, consecutiveFailures)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
		}

		log.Printf("opencode: creating event stream...")
		stream := c.sdk.Event.ListStreaming(ctx, opencode.EventListParams{})

		eventCount := 0
		connected := false

		for stream.Next() {
			event := stream.Current()
			eventCount++

			eventType := string(event.Type)

			// 棣栨鏀跺埌浜嬩欢锛屾爣璁拌繛鎺ユ垚鍔?
			if !connected {
				connected = true
				if consecutiveFailures > 0 {
					log.Printf("opencode: 馃攧 server reconnected after %d failures, invalidating stale session cache", consecutiveFailures)
					c.invalidateStaleSessions(ctx)
					log.Printf("opencode: 鉁?event listener reconnected successfully after %d failures", consecutiveFailures)
				}
				consecutiveFailures = 0
			}

			// Log every event for debugging
			if eventCount <= 10 || eventType != "server.heartbeat" {
				log.Printf("opencode: [event #%d] type=%s", eventCount, eventType)
			}

			// Extract session ID using shared helper
			sessionID := extractSessionIDFromEvent(&event)

			// Populate messageRoles for message.part.updated events (regardless of sessionID path)
			// so that message.part.delta events for user messages can be filtered before broadcast.
			if eventType == "message.part.updated" {
				if raw := event.JSON.RawJSON(); raw != "" {
					// Dump first 15 events for debugging
					if eventCount <= 15 {
						log.Printf("opencode: 馃攳馃攳 [message.part.updated #%d RAW] %.1000s", eventCount, raw)
					}
					var msgInfo struct {
						Properties struct {
							Message struct {
								ID   string `json:"id"`
								Role string `json:"role"`
							} `json:"message"`
						} `json:"properties"`
					}
					if json.Unmarshal([]byte(raw), &msgInfo) == nil {
						if msgID := msgInfo.Properties.Message.ID; msgID != "" && msgInfo.Properties.Message.Role != "" {
							c.messageRoles.Store(msgID, msgInfo.Properties.Message.Role)
						}
					}
				}
			}

			// ALSO populate messageRoles from message.updated events (which DO contain role)
			// NOTE: message.updated structure is properties.info.{id,role} NOT properties.message.{id,role}
			if eventType == "message.updated" {
				if raw := event.JSON.RawJSON(); raw != "" {
					var msgInfo struct {
						Properties struct {
							Info struct {
								ID   string `json:"id"`
								Role string `json:"role"`
							} `json:"info"`
						} `json:"properties"`
					}
					if json.Unmarshal([]byte(raw), &msgInfo) == nil {
						if msgID := msgInfo.Properties.Info.ID; msgID != "" && msgInfo.Properties.Info.Role != "" {
							c.messageRoles.Store(msgID, msgInfo.Properties.Info.Role)
							log.Printf("opencode: 鉁?stored messageRole: msgID=%s, role=%s",
								msgID[:min(8, len(msgID))], msgInfo.Properties.Info.Role)
						}
					}
				}
			}

			// For message.part.updated / message.part.delta events the base extractor
			// may return "" because the sessionID is nested in properties.message.
			// Re-parse those two types and also build the messageID鈫抯essionID reverse map
			// so that subsequent delta events can be dispatched correctly.
			if sessionID == "" && (eventType == "message.part.delta" || eventType == "message.part.updated") {
				if raw := event.JSON.RawJSON(); raw != "" {
					var probe struct {
						Properties struct {
							MessageID string `json:"messageID"` // present in message.part.delta
							Message   struct {
								ID        string `json:"id"`
								SessionID string `json:"sessionID"`
								Role      string `json:"role"` // "user" or "assistant"
							} `json:"message"` // present in message.part.updated
						} `json:"properties"`
					}
					if json.Unmarshal([]byte(raw), &probe) == nil {
						if probe.Properties.Message.SessionID != "" {
							// message.part.updated 鈥?record messageID鈫抯essionID and messageID鈫抮ole
							sessionID = probe.Properties.Message.SessionID
							if msgID := probe.Properties.Message.ID; msgID != "" {
								c.messageToSession.Store(msgID, sessionID)
								if role := probe.Properties.Message.Role; role != "" {
									c.messageRoles.Store(msgID, role)
								}
							}
						} else if probe.Properties.MessageID != "" {
							// message.part.delta 鈥?look up sessionID via reverse map
							if sid, ok := c.messageToSession.Load(probe.Properties.MessageID); ok {
								sessionID = sid.(string)
							}
							// If still no sessionID, broadcast 鈥?but only for assistant messages
							// with a KNOWN role. Unknown-role deltas are likely from old/other
							// sessions being replayed; broadcasting them would echo stale content.
							if sessionID == "" {
								role, roleKnown := c.messageRoles.Load(probe.Properties.MessageID)
								if !roleKnown {
									log.Printf("opencode: dropping message.part.delta with unknown messageID (likely old session replay)")
								} else if role.(string) == "user" {
									log.Printf("opencode: skipping user message.part.delta broadcast (msgID=%s)", probe.Properties.MessageID[:min(8, len(probe.Properties.MessageID))])
								} else {
									// Known assistant message 鈥?broadcast to all session handlers
									dispatched := 0
									c.sessionHandlers.Range(func(k, v interface{}) bool {
										if err := v.(EventHandler)(ctx, &event); err != nil {
											log.Printf("opencode: message.part.delta broadcast error (session %v): %v", k, err)
										}
										dispatched++
										return true
									})
									if dispatched == 0 {
										log.Printf("opencode: message.part.delta broadcast - no session handlers registered")
									}
								}
							}
						}
					}
					// Dump first few events for structure verification
					if eventCount <= 5 {
						log.Printf("opencode: [raw %s #%d] %.300s", eventType, eventCount, raw)
					}
				}
			}

			if sessionID != "" && len(sessionID) > 8 {
				log.Printf("opencode: processing event type=%s, sessionID=%s", eventType, sessionID[:8])
			} else if eventType != "server.heartbeat" && eventType != "server.connected" && sessionID == "" {
				log.Printf("opencode: processing event type=%s (no sessionID)", eventType)
			}

			// Dispatch to the specific session handler (when sessionID is known)
			if sessionID != "" {
				if handler, ok := c.sessionHandlers.Load(sessionID); ok {
					if err := handler.(EventHandler)(ctx, &event); err != nil {
						log.Printf("opencode: session handler error for %s: %v", sessionID[:8], err)
					}
				} else {
					log.Printf("opencode: no session handler found for %s", sessionID[:8])
					c.sessionHandlers.Range(func(k, v interface{}) bool {
						log.Printf("opencode:   registered handler: %v", k)
						return true
					})
				}
			}

			// Always dispatch to global handlers
			c.eventListenerMu.RLock()
			handlers := c.eventHandlers
			c.eventListenerMu.RUnlock()

			if len(handlers) > 0 {
				log.Printf("opencode: dispatching to %d global handlers", len(handlers))
			}

			for _, handler := range handlers {
				if err := handler(ctx, &event); err != nil {
					log.Printf("opencode: global event handler error: %v", err)
				}
			}
		}

		stream.Close()

		log.Printf("opencode: event stream ended, total events processed: %d", eventCount)

		if err := stream.Err(); err != nil {
			log.Printf("opencode: event stream error: %v", err)
			c.maybeHandleServerUnavailable(ctx, err, "event-stream")
		}

		// 妫€鏌ユ槸鍚︽槸涓诲姩閫€鍑?
		select {
		case <-ctx.Done():
			log.Printf("opencode: event listener stopped (context cancelled)")
			return
		default:
			// 闈炰富鍔ㄩ€€鍑猴紝鍑嗗閲嶈繛
			consecutiveFailures++
			log.Printf("opencode: 鈿狅笍 event stream disconnected unexpectedly, will reconnect...")
		}
	}
}

func (c *Client) maybeHandleServerUnavailable(ctx context.Context, err error, source string) {
	if c.serverUnavailable == nil || err == nil {
		return
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "connection refused") &&
		!strings.Contains(msg, "no such host") &&
		!strings.Contains(msg, "dial tcp") &&
		!strings.Contains(msg, "connect:") {
		return
	}

	c.unavailableMu.Lock()
	if !c.lastUnavailableAt.IsZero() && time.Since(c.lastUnavailableAt) < c.unavailableDelay {
		c.unavailableMu.Unlock()
		return
	}
	c.lastUnavailableAt = time.Now()
	c.unavailableMu.Unlock()

	reason := fmt.Sprintf("%s unavailable: %v", source, err)
	go func() {
		actionCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		msg, actionErr := c.serverUnavailable(actionCtx, reason)
		if actionErr != nil {
			log.Printf("opencode: self-heal action failed (%s): %v", source, actionErr)
			return
		}
		if strings.TrimSpace(msg) != "" {
			log.Printf("opencode: self-heal action: %s", msg)
		}
	}()
}

// invalidateStaleSessions 娓呴櫎鍙兘澶辨晥鐨?session 缂撳瓨
// 褰?OpenCode Server 閲嶅惎鍚庤皟鐢紝璁╀笅娆℃秷鎭椂鍒涘缓鏂?session
func (c *Client) invalidateStaleSessions(ctx context.Context) {
	var staleThreads []string

	c.sessions.Range(func(key, value interface{}) bool {
		threadID := key.(string)
		sessionID := value.(string)

		// 灏濊瘯楠岃瘉 session 鏄惁浠嶇劧鏈夋晥
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := c.GetSession(checkCtx, sessionID)
		cancel()

		if err != nil {
			log.Printf("opencode: 馃棏锔?session %s for thread %s is stale (err: %v), removing",
				sessionID[:min(8, len(sessionID))], threadID, err)
			staleThreads = append(staleThreads, threadID)
		} else {
			log.Printf("opencode: 鉁?session %s for thread %s is still valid",
				sessionID[:min(8, len(sessionID))], threadID)
		}

		return true
	})

	// 鍒犻櫎澶辨晥鐨勬槧灏?
	for _, threadID := range staleThreads {
		c.sessions.Delete(threadID)
		log.Printf("opencode: removed stale session mapping for thread %s", threadID)
	}

	if len(staleThreads) > 0 {
		log.Printf("opencode: invalidated %d stale session mappings", len(staleThreads))
	}
}

// RegisterEventHandler adds a new event handler dynamically.
func (c *Client) RegisterEventHandler(handler EventHandler) {
	c.eventListenerMu.Lock()
	defer c.eventListenerMu.Unlock()
	c.eventHandlers = append(c.eventHandlers, handler)
}

// RegisterSessionHandler registers an event handler tied to a specific session.
// This allows fast lookup and prevents unnecessary processing of unrelated events.
func (c *Client) RegisterSessionHandler(sessionID string, handler EventHandler) {
	c.sessionHandlers.Store(sessionID, handler)
}

// UnregisterSessionHandler removes an event handler for a specific session.
func (c *Client) UnregisterSessionHandler(sessionID string) {
	c.sessionHandlers.Delete(sessionID)
	c.activeHandlers.Delete(sessionID)
}

// GetTodosForSession returns the current todo list for a running session.
// Returns nil if the session has no active handler or no todos.
func (c *Client) GetTodosForSession(sessionID string) []TodoItem {
	if h, ok := c.activeHandlers.Load(sessionID); ok {
		return h.(*StreamingSessionHandler).GetTodos()
	}
	return nil
}

// GetDiffForSession returns the accumulated file-change diff for a session.
// Returns nil if the session has no active handler or no diff yet.
func (c *Client) GetDiffForSession(sessionID string) []FileDiff {
	if h, ok := c.activeHandlers.Load(sessionID); ok {
		return h.(*StreamingSessionHandler).GetDiff()
	}
	return nil
}

var _ MessageSender = (*Client)(nil)

// SendMessageToSession 閫氳繃adapter涓诲姩鎺ㄩ€佹秷鎭粰浼氳瘽鍏宠仈鐨勭敤鎴?
// 娉ㄦ剰锛氳繖涓柟娉曠洰鍓嶅彧鏄褰曟棩蹇楋紝瀹為檯鐨勬秷鎭帹閫侀€氳繃streaming callback瀹屾垚
// 鏈潵鍙互鎵╁睍鏀寔閫氳繃adapter鐨勫弻鍚戦€氫俊鏈哄埗涓诲姩鎺ㄩ€?
func (c *Client) SendMessageToSession(ctx context.Context, sessionID, content string) error {
	log.Printf("opencode: SendMessageToSession for session %s (len=%d chars)", sessionID[:8], len(content))
	// 褰撳墠瀹炵幇锛氫緷璧杝treaming callback鏈哄埗锛岃繖閲屽彧鏄帴鍙ｅ崰浣?
	// 鏈潵鍙互娣诲姞閫氳繃adapter鍙嶅悜鎺ㄩ€佺殑閫昏緫
	return nil
}

// extractReplyFromMessage extracts text content from a prompt response.
// OpenCode杩斿洖鐨刴essage鍖呭惈澶氫釜part锛屾瘡涓猵art鍙互鏄細
// - TextPart: 鏅€氭枃鏈?
// - 鍏朵粬绫诲瀷: 宸ュ叿璋冪敤銆佹€濊€冭繃绋嬬瓑
func extractReplyFromMessage(msg *opencode.SessionPromptResponse) string {
	if msg == nil || len(msg.Parts) == 0 {
		log.Printf("opencode: WARNING - no response parts to extract")
		return "(processing, please check OpenCode UI for result)"
  }

	var textParts []string

	for i, part := range msg.Parts {
		switch p := part.AsUnion().(type) {
		case opencode.TextPart:
			textParts = append(textParts, p.Text)
			log.Printf("opencode: extracted text part %d: %d chars", i, len(p.Text))
		default:
			// 鍏朵粬绫诲瀷鐨刾art锛岃褰曚絾涓嶆彁鍙?
			log.Printf("opencode: skipped non-text part %d (type: %T)", i, p)
		}
	}

	if len(textParts) == 0 {
		log.Printf("opencode: WARNING - no text parts found in %d parts", len(msg.Parts))
		return "(鍝嶅簲宸叉敹鍒颁絾鏃犳枃鏈唴瀹癸紝璇锋煡鐪?OpenCode 鐣岄潰 - message ID: " + msg.Info.ID + ")"
	}

	fullReply := strings.Join(textParts, "\n")
	log.Printf("opencode: extracted reply: %d chars from %d text parts", len(fullReply), len(textParts))
	return fullReply
}

// getThreadLock gets or creates a lock for a specific thread to prevent concurrent operations.
func (c *Client) getThreadLock(threadID string) *sync.Mutex {
	if threadID == "" {
		return &sync.Mutex{} // Return a new mutex for single-use
	}

	lock, _ := c.sessionLocks.LoadOrStore(threadID, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// SummarizeSession 鎬荤粨涓€涓猻ession鐨勫璇濆唴瀹?
func (c *Client) SummarizeSession(ctx context.Context, sessionID string) error {
	if !c.Ready() {
		return fmt.Errorf("opencode: client not configured")
	}

	// 妫€鏌ユ槸鍚﹀凡鏈夋€荤粨
	if _, exists := c.sessionSummary.Load(sessionID); exists {
		return nil // 宸茬粡鎬荤粨杩囦簡
	}

	log.Printf("opencode: summarizing session %s", sessionID)

	// 璋冪敤OpenCode鐨剆ummarize API
	_, err := c.sdk.Session.Summarize(ctx, sessionID, opencode.SessionSummarizeParams{})
	if err != nil {
		return fmt.Errorf("opencode: summarize session: %w", err)
	}

	// 鑾峰彇session璇︽儏浠ヨ幏鍙栨€荤粨
	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("opencode: get session after summarize: %w", err)
	}

	// 鎻愬彇鎬荤粨鍐呭锛堜粠session鐨刴essages涓煡鎵緎ummary绫诲瀷鐨勬秷鎭級
	summary := extractSummaryFromSession(session)
	if summary != "" {
		c.sessionSummary.Store(sessionID, summary)
		log.Printf("opencode: session %s summarized successfully", sessionID)
	}

	return nil
}

// extractSummaryFromSession 浠巗ession涓彁鍙栨€荤粨淇℃伅
func extractSummaryFromSession(session *opencode.Session) string {
	if session == nil {
		return ""
	}
	// TODO: 鏍规嵁瀹為檯鐨剆ession缁撴瀯鎻愬彇鎬荤粨
	// 鍙兘闇€瑕佽幏鍙杕essages骞舵煡鎵緎ummary绫诲瀷鐨勬秷鎭?
	return "" // 鏆傛椂杩斿洖绌猴紝闇€瑕佹牴鎹甋DK瀹為檯缁撴瀯瀹炵幇
}

// truncateString 鎴柇瀛楃涓插埌鎸囧畾闀垮害
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	// 澶勭悊UTF-8瀛楃
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}

// GetMessageCount 鑾峰彇鎸囧畾session鐨勬秷鎭暟閲?
func (c *Client) GetMessageCount(sessionID string) int {
	return c.loadCounter(&c.messageCount, sessionID)
}

// ResetSession 閲嶇疆thread鐨剆ession鏄犲皠锛屽己鍒跺垱寤烘柊session
func (c *Client) ResetSession(threadID string) {
	if threadID != "" {
		c.sessions.Delete(threadID)
		log.Printf("opencode: reset session mapping for thread %s", threadID)
	}
}

// GetSessionForThread retrieves the session ID associated with a thread.
func (c *Client) GetSessionForThread(threadID string) (string, bool) {
	val, ok := c.sessions.Load(threadID)
	if !ok {
		return "", false
	}
	return val.(string), true
}

// GetSessionInfo retrieves detailed information about a session.
type SessionInfo struct {
	SessionID     string
	Title         string
	Directory     string
	MessageCount  int
	TokenCount    int
	ContextUsage  float64
	ContextLength int
	Created       string // Use string since SDK doesn't have CreatedAt
}

func (c *Client) GetSessionInfo(ctx context.Context, sessionID string) (*SessionInfo, error) {
	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	// Convert Unix timestamp to time string
	createdTime := time.Unix(int64(session.Time.Updated), 0).Format("2006-01-02 15:04:05")

	info := &SessionInfo{
		SessionID:    sessionID,
		Title:        session.Title,
		Directory:    session.Directory,
		MessageCount: c.GetMessageCount(sessionID),
		TokenCount:   c.GetTokenCount(sessionID),
		Created:      createdTime,
	}

	// Get context length
	info.ContextLength = c.getMaxContextLength(sessionID)
	if info.ContextLength > 0 && info.TokenCount > 0 {
		info.ContextUsage = float64(info.TokenCount) / float64(info.ContextLength)
	}

	return info, nil
}

// SendMessageStreaming sends a message and calls the callback for each chunk of the response.
// 鐪熸鐨勬祦寮忓疄鐜帮細娉ㄥ唽StreamingSessionHandler鐩戝惉瀹炴椂浜嬩欢
func (c *Client) SendMessageStreaming(ctx context.Context, payload MessagePayload, callback StreamCallback) (Response, error) {
  return c.SendMessageStreamingWithEvents(ctx, payload, callback, nil)
}

// SendMessageStreamingWithEvents sends a streaming message with both legacy chunk callback
// and structured event callback.
func (c *Client) SendMessageStreamingWithEvents(ctx context.Context, payload MessagePayload, callback StreamCallback, eventCallback StreamEventCallback) (Response, error) {
	//fmt.Println("payload is______________________________:", payload.)
	if callback == nil {
		// 濡傛灉娌℃湁鍥炶皟锛岀洿鎺ヤ娇鐢ㄦ櫘閫氭ā寮?
		return c.SendMessage(ctx, payload)
	}

	// 1. 鍏堢‘瀹歴essionID锛堝彲鑳介渶瑕佸垱寤烘柊session锛?
	threadLock := c.getThreadLock(payload.ThreadID)
	threadLock.Lock()
	sessionID := payload.SessionID
	if sessionID == "" && payload.ThreadID != "" {
		if sid, ok := c.sessions.Load(payload.ThreadID); ok {
			sessionID = sid.(string)

			// 楠岃瘉 session 鏄惁浠嶇劧鏈夋晥
			checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, checkErr := c.GetSession(checkCtx, sessionID)
			checkCancel()
			if checkErr != nil {
				log.Printf("opencode: 鈿狅笍 streaming session %s is stale (err: %v), will create new",
					sessionID[:min(8, len(sessionID))], checkErr)
				c.sessions.Delete(payload.ThreadID)
				c.messageCount.Delete(sessionID)
				c.tokenCount.Delete(sessionID)
				sessionID = "" // 寮哄埗鍒涘缓鏂?session
			}
		}
	}

	// 濡傛灉杩樻槸娌℃湁sessionID锛屾垜浠渶瑕佸厛鍒涘缓session
	if sessionID == "" {
		sessionCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		// 灏?adapter 鍜?user 淇℃伅缂栫爜鍒?Title 涓紝鏍煎紡: [adapter:userId] threadId
		sessionTitle := fmt.Sprintf("[%s:%s] %s", payload.Channel, payload.UserID, payload.ThreadID)
		session, err := c.sdk.Session.New(sessionCtx, opencode.SessionNewParams{
			Title: opencode.F(sessionTitle),
		})
		cancel()
		if err != nil {
			threadLock.Unlock()
			return Response{}, fmt.Errorf("opencode: create session for streaming: %w", err)
		}
		sessionID = session.ID
		if payload.ThreadID != "" {
			c.sessions.Store(payload.ThreadID, sessionID)
		}
		c.messageCount.Store(sessionID, 0)
		c.tokenCount.Store(sessionID, 0)
		log.Printf("opencode: created new session %s for streaming", sessionID)
	}
	threadLock.Unlock()

	// 2. 绔嬪嵆閫氳繃callback閫氱煡sessionID锛堜緵adapter寤虹珛user鏄犲皠锛?
	log.Printf("opencode: notifying sessionID %s via callback", sessionID)
	if err := callback(sessionID); err != nil {
		log.Printf("opencode: failed to notify sessionID via callback: %v", err)
	} else {
		log.Printf("opencode: sessionID notification sent successfully")
	}

	// 3. 鍒涘缓StreamingSessionHandler骞舵敞鍐?
	handler := NewStreamingSessionHandler(sessionID, callback, eventCallback, func() {
		c.runningSessions.Delete(sessionID)
		c.UnregisterSessionHandler(sessionID)
	}, c, c, c.IsThinkingEnabled(), c.IsStepEnabled())
	c.RegisterSessionHandler(sessionID, handler.HandleEvent)
	c.activeHandlers.Store(sessionID, handler)
	log.Printf("opencode: registered streaming handler for session %s", sessionID[:8])

	// 4. 浣跨敤goroutine寮傛鍙戦€佹秷鎭?
	responseChan := make(chan Response, 1)
	errorChan := make(chan error, 1)

	go func() {
		response, err := c.SendMessage(ctx, payload)
		if err != nil {
			errorChan <- err
			return
		}
		responseChan <- response
	}()

	// 4. 瀹氭椂妫€鏌ュ畬鎴愮姸鎬侊紙涓嶅啀鍙戦€佽繘搴︽秷鎭級
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	isAsyncMode := false
	var asyncResponse Response
	idleCheckCount := 0 // 绌洪棽妫€娴嬭鏁板櫒

	for {
		select {
		case <-ctx.Done():
			return Response{}, ctx.Err()

		case err := <-errorChan:
			return Response{}, err

		case response := <-responseChan:
			// 濡傛灉鏄疉sync妯″紡锛坮eply涓虹┖锛夛紝鏍囪骞剁户缁瓑寰匰SE浜嬩欢
			if response.Reply == "" {
				log.Printf("opencode: streaming async mode for session %s, waiting for SSE events", sessionID[:8])
				isAsyncMode = true
				asyncResponse = response
				continue
			}

			// 鍚屾妯″紡锛氭鏌ユ槸鍚﹂€氳繃streaming handler宸茬粡鍙戦€佷簡鍐呭
			sentContent := handler.GetLastContent()
			log.Printf("opencode: streaming completed, handler sent: %d chars, final response: %d chars",
				len(sentContent), len(response.Reply))

			if sentContent == "" || sentContent != response.Reply {
				if response.Reply != "" {
					log.Printf("opencode: sending final response via callback")
					if err := callback(response.Reply); err != nil {
						log.Printf("opencode: final callback error: %v", err)
					}
				}
			}
			return response, nil

		case <-ticker.C:
			// 濡傛灉鍦╝sync妯″紡涓攈andler宸插畬鎴愶紝杩斿洖缁撴灉
			if isAsyncMode && handler.IsCompleted() {
				log.Printf("opencode: 鉁?async streaming completed via SSE for session %s (contentSent=%t, lastContentLen=%d)",
					sessionID[:8], handler.HasSentContent(), len(handler.GetLastContent()))
				// 娉ㄦ剰锛氫笉濉厖 asyncResponse.Reply锛岃璋冪敤鑰呬粠 fullReply 鑾峰彇鍐呭
				return asyncResponse, nil
			}

			// 妫€鏌ユ渶鍚庝竴娆′簨浠舵椂闂?
			lastEventTime, lastEventType := handler.GetLastEventInfo()
			timeSinceLastEvent := time.Since(lastEventTime)
			hasSentContent := handler.HasSentContent()
			hasStepFinish := handler.HasReceivedStepFinish()
			stepFinishTime := handler.GetStepFinishTime()
			isCompleted := handler.IsCompleted()

			log.Printf("opencode: 馃攳 ticker check - session=%s, isAsync=%t, isCompleted=%t, hasSent=%t, hasStepFinish=%t, lastEvent=%v ago (type=%s), idleCount=%d",
				sessionID[:8], isAsyncMode, isCompleted, hasSentContent, hasStepFinish, timeSinceLastEvent, lastEventType, idleCheckCount)

			// 濡傛灉鏀跺埌浜?step-finish 浜嬩欢涓斿凡鍙戦€佸唴瀹癸紝5绉掑悗娌℃湁鏂颁簨浠跺氨璁や负瀹屾垚
			// (step-finish 閫氬父鏍囧織鐫€妯″瀷杈撳嚭瀹屾垚锛屽悗缁簲璇ュ緢蹇湁 session.idle)
			if isAsyncMode && hasStepFinish && hasSentContent && !stepFinishTime.IsZero() {
				timeSinceStepFinish := time.Since(stepFinishTime)
				if timeSinceStepFinish > 5*time.Second {
					log.Printf("opencode: 馃弫 received step-finish %v ago (has sent content), treating as completed for session %s",
						timeSinceStepFinish, sessionID[:8])
					return asyncResponse, nil
				}
			}

			// 濡傛灉宸插彂閫佸唴瀹逛笖瓒呰繃30绉掓棤鏂颁簨浠讹紝璁や负鍙兘瀹屾垚
			// 锛堜粠2鍒嗛挓缂╃煭鍒?0绉掞紝鏇村揩鍝嶅簲锛?
			if isAsyncMode && hasSentContent && timeSinceLastEvent > 30*time.Second {
				log.Printf("opencode: 鈴憋笍 streaming idle for %v (has sent content), treating as completed for session %s",
					timeSinceLastEvent, sessionID[:8])
				return asyncResponse, nil
			}

			// 濡傛灉瓒呰繃1鍒嗛挓鏃犱换浣曚簨浠讹紙鍗充娇娌″彂閫佸唴瀹癸級锛屼篃璁や负瀹屾垚
			// 杩欏鐞?OpenCode 涓嶅彂閫佸畬鎴愪簨浠剁殑鎯呭喌
			if isAsyncMode && timeSinceLastEvent > 1*time.Minute {
				log.Printf("opencode: 鈴憋笍 streaming timeout (no events for %v, hasSent=%t), treating as completed for session %s",
					timeSinceLastEvent, hasSentContent, sessionID[:8])
				return asyncResponse, nil
			}

			idleCheckCount++
		}
	}
}

// estimateTokens 浼扮畻鏂囨湰鐨則oken鏁伴噺
// 绠€鍗曞疄鐜帮細涓枃瀛楃鎸?.5鍊嶈绠楋紝鑻辨枃鍗曡瘝鎸?涓猼oken璁＄畻
func estimateTokens(text string) int {
	if text == "" {
		return 0
	}

	runes := []rune(text)
	tokens := 0
	inWord := false

	for _, r := range runes {
		// 涓枃瀛楃锛圕JK缁熶竴琛ㄦ剰鏂囧瓧锛?
		if r >= 0x4E00 && r <= 0x9FFF {
			tokens += 2 // 涓枃瀛楃閫氬父鍗犵敤鏇村token
		} else if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			// 鑻辨枃瀛楁瘝鍜屾暟瀛楋紝鎸夊崟璇嶈鏁?
			if !inWord {
				tokens++
				inWord = true
			}
		} else {
			inWord = false
			// 鏍囩偣绗﹀彿绛?
			if r != ' ' && r != '\t' && r != '\n' {
				tokens++
			}
		}
	}

	// 娣诲姞涓€浜涘紑閿€锛堢郴缁熸彁绀鸿瘝銆佹牸寮忓寲绛夛級
	return int(float64(tokens) * 1.3)
}

// getMaxContextLength 鑾峰彇session鐨勬渶澶т笂涓嬫枃闀垮害
func (c *Client) getMaxContextLength(sessionID string) int {
	// 灏濊瘯浠庣紦瀛樿幏鍙栨ā鍨嬮厤缃?
	if cfg, ok := c.modelConfig.Load(sessionID); ok {
		modelCfg := cfg.(*ModelConfig)
		if modelCfg.ContextLength > 0 {
			return modelCfg.ContextLength
		}
	}

	// 杩斿洖榛樿鍊?
	return DefaultMaxTokens
}

// fetchAndCacheModelConfig 鑾峰彇骞剁紦瀛榮ession鐨勬ā鍨嬮厤缃?
func (c *Client) fetchAndCacheModelConfig(ctx context.Context, sessionID string) {
	// 鍒涘缓涓€涓甫瓒呮椂鐨刢ontext
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 鑾峰彇session璇︽儏
	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		log.Printf("opencode: failed to get session %s for model config: %v", sessionID[:8], err)
		return
	}

	// 鎻愬彇妯″瀷淇℃伅锛堥渶瑕佹牴鎹疄闄匰DK缁撴瀯璋冩暣锛?
	// 杩欓噷鍋囪session涓寘鍚ā鍨嬩俊鎭紝瀹為檯鍙兘闇€瑕佽皟鐢ㄥ叾浠朅PI
	config := &ModelConfig{
		LastUpdated:   time.Now(),
		ContextLength: guessContextLengthFromSession(session),
	}

	c.modelConfig.Store(sessionID, config)
	log.Printf("opencode: cached model config for session %s, context length: %d",
		sessionID[:8], config.ContextLength)
}

// guessContextLengthFromSession 鏍规嵁session淇℃伅鐚滄祴涓婁笅鏂囬暱搴?
func guessContextLengthFromSession(session *opencode.Session) int {
	// TODO: 鏍规嵁瀹為檯SDK缁撴瀯鎻愬彇妯″瀷淇℃伅
	// 鍙兘闇€瑕佽皟鐢?/config/providers API 鑾峰彇妯″瀷鍒楄〃鍜岄厤缃?

	// 甯歌妯″瀷鐨勪笂涓嬫枃闀垮害
	// GPT-4: 8k, 32k, 128k
	// Claude: 100k, 200k
	// 鍏朵粬妯″瀷: 4k-8k

	// 鐩墠杩斿洖涓€涓繚瀹堢殑榛樿鍊?
	return 8192 // 8k tokens锛岄€傜敤浜庡ぇ澶氭暟妯″瀷
}

// GetTokenCount 鑾峰彇鎸囧畾session鐨則oken浣跨敤閲?
func (c *Client) GetTokenCount(sessionID string) int {
	return c.loadCounter(&c.tokenCount, sessionID)
}

// loadCounter safely loads an integer counter from sync.Map.
// It auto-recovers missing or unexpected value types by normalizing to 0.
func (c *Client) loadCounter(counterMap *sync.Map, key string) int {
	v, ok := counterMap.Load(key)
	if !ok || v == nil {
		counterMap.Store(key, 0)
		return 0
	}
	if n, ok := v.(int); ok {
		return n
	}

	// 鍏煎鍘嗗彶/寮傚父鍊硷紝閬垮厤 interface conversion panic
	switch n := v.(type) {
	case int8:
		counterMap.Store(key, int(n))
		return int(n)
	case int16:
		counterMap.Store(key, int(n))
		return int(n)
	case int32:
		counterMap.Store(key, int(n))
		return int(n)
	case int64:
		counterMap.Store(key, int(n))
		return int(n)
	case uint:
		counterMap.Store(key, int(n))
		return int(n)
	case uint8:
		counterMap.Store(key, int(n))
		return int(n)
	case uint16:
		counterMap.Store(key, int(n))
		return int(n)
	case uint32:
		counterMap.Store(key, int(n))
		return int(n)
	case uint64:
		counterMap.Store(key, int(n))
		return int(n)
	case float32:
		counterMap.Store(key, int(n))
		return int(n)
	case float64:
		counterMap.Store(key, int(n))
		return int(n)
	default:
		log.Printf("opencode: counter type mismatch for key %s, got %T, reset to 0", key, v)
		counterMap.Store(key, 0)
		return 0
	}
}

// incrementCounter adds delta to a counter with safe initialization.
func (c *Client) incrementCounter(counterMap *sync.Map, key string, delta int) {
	current := c.loadCounter(counterMap, key)
	counterMap.Store(key, current+delta)
}

// GetContextUsage 鑾峰彇session鐨勪笂涓嬫枃浣跨敤鐜?
func (c *Client) GetContextUsage(sessionID string) float64 {
	tokens := c.GetTokenCount(sessionID)
	maxTokens := c.getMaxContextLength(sessionID)
	if maxTokens == 0 {
		return 0
	}
	return float64(tokens) / float64(maxTokens)
}

// ========== 閲嶈瘯鏈哄埗 ==========

// isRetryableError 鍒ゆ柇閿欒鏄惁鍙互閲嶈瘯
func (c *Client) isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	for _, keyword := range c.retryConfig.RetryableErrors {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(keyword)) {
			return true
		}
	}
	return false
}

// calculateBackoff 璁＄畻鎸囨暟閫€閬垮欢杩?
func (c *Client) calculateBackoff(attempt int) time.Duration {
	delay := c.retryConfig.InitialDelay * time.Duration(1<<uint(attempt))
	if delay > c.retryConfig.MaxDelay {
		delay = c.retryConfig.MaxDelay
	}
	// 娣诲姞闅忔満鎶栧姩闃叉鎯婄兢鏁堝簲
	jitter := time.Duration(rand.Int63n(int64(delay / 4)))
	return delay + jitter
}

// sendPromptWithRetry 甯﹂噸璇曠殑鍙戦€佹秷鎭?
func (c *Client) sendPromptWithRetry(ctx context.Context, sessionID string, parts []opencode.SessionPromptParamsPartUnion, model *opencode.SessionPromptParamsModel) (*opencode.SessionPromptResponse, error) {
	var lastErr error

	for attempt := 0; attempt <= c.retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := c.calculateBackoff(attempt - 1)
			log.Printf("opencode: retry attempt %d/%d for session %s after %v delay",
				attempt, c.retryConfig.MaxRetries, sessionID[:8], delay)

			// 绛夊緟閲嶈瘯寤惰繜锛屼笉妫€鏌ュ閮╟ontext锛堝畠鍙兘宸茶秴鏃讹級
			time.Sleep(delay)
		}

		// 涓烘瘡娆″皾璇曞垱寤虹嫭绔嬬殑context锛岄伩鍏嶅墠涓€娆¤秴鏃跺奖鍝嶄笅涓€娆?
		attemptCtx, cancel := context.WithTimeout(context.Background(), c.timeout)

		params := opencode.SessionPromptParams{
			Parts: opencode.F(parts),
		}
		if model != nil {
			params.Model = opencode.F(*model)
		}

		result, err := c.sdk.Session.Prompt(attemptCtx, sessionID, params)
		cancel()

		if err == nil {
			// 璁板綍鍝嶅簲璇︽儏
			if result != nil {
				log.Printf("opencode: received response for session %s - parts: %d, message_id: %s",
					sessionID[:8], len(result.Parts), result.Info.ID)
				// 妫€鏌ュ搷搴旀槸鍚︿负绌?
				if len(result.Parts) == 0 {
					log.Printf("opencode: WARNING - empty response parts for session %s", sessionID[:8])
				}
			} else {
				log.Printf("opencode: WARNING - nil result for session %s", sessionID[:8])
			}
			if attempt > 0 {
				log.Printf("opencode: retry succeeded on attempt %d for session %s", attempt, sessionID[:8])
			}
			return result, nil
		}

		lastErr = err

		// 鍒ゆ柇鏄惁鍙噸璇?
		if !c.isRetryableError(err) {
			log.Printf("opencode: non-retryable error for session %s: %v", sessionID[:8], err)
			return nil, err
		}

		log.Printf("opencode: retryable error on attempt %d for session %s: %v", attempt, sessionID[:8], err)

		// 濡傛灉鏄渶鍚庝竴娆″皾璇曞墠锛岀粰鍑虹壒鍒彁绀?
		if attempt == c.retryConfig.MaxRetries {
			log.Printf("opencode: FINAL retry attempt failed for session %s. Task may require user interaction in OpenCode UI.", sessionID[:8])
		}
	}

	return nil, fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
}

func modelCatalogKey(providerID, modelID string) string {
	return strings.ToLower(strings.TrimSpace(providerID)) + "/" + strings.ToLower(strings.TrimSpace(modelID))
}

func (c *Client) mediaDebugf(format string, args ...interface{}) {
	if c == nil || !c.debugMediaRouting {
		return
	}
	log.Printf("opencode[media]: "+format, args...)
}

func (c *Client) refreshModelCatalog(ctx context.Context) error {
	providers, err := c.GetProviders(ctx)
	if err != nil {
		return err
	}
	log.Printf("opencode: model catalog refreshed, providers=%d", len(providers))
	return nil
}

func (c *Client) ensureModelCatalog(ctx context.Context) {
	c.modelCatalogMu.RLock()
	empty := len(c.modelCatalog) == 0
	c.modelCatalogMu.RUnlock()
	c.mediaDebugf("ensure catalog: empty=%t", empty)
	if !empty {
		return
	}
	if err := c.refreshModelCatalog(ctx); err != nil {
		log.Printf("opencode: ensure model catalog failed: %v", err)
		c.mediaDebugf("catalog refresh failed: %v", err)
	}
}

func (c *Client) updateSessionModel(sessionID, providerID, modelID string) {
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(providerID) == "" || strings.TrimSpace(modelID) == "" {
		return
	}

	cfg := &ModelConfig{
		ProviderID:  providerID,
		ModelID:     modelID,
		LastUpdated: time.Now(),
	}
	c.sessionModel.Store(sessionID, cfg)

	if v, ok := c.modelConfig.Load(sessionID); ok {
		existing := v.(*ModelConfig)
		existing.ProviderID = providerID
		existing.ModelID = modelID
		existing.LastUpdated = time.Now()
		c.modelConfig.Store(sessionID, existing)
	}
}

func (c *Client) getCurrentSessionModel(ctx context.Context, sessionID string) (*ModelConfig, bool) {
	if v, ok := c.sessionModel.Load(sessionID); ok {
		cfg := v.(*ModelConfig)
		if cfg.ProviderID != "" && cfg.ModelID != "" {
			return cfg, true
		}
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	messages, err := c.sdk.Session.Messages(lookupCtx, sessionID, opencode.SessionMessagesParams{})
	if err != nil || messages == nil || len(*messages) == 0 {
		c.defaultModelMu.RLock()
		defer c.defaultModelMu.RUnlock()
		if c.defaultModel != nil {
			return c.defaultModel, true
		}
		return nil, false
	}

	for i := len(*messages) - 1; i >= 0; i-- {
		msg := (*messages)[i].Info
		if msg.ProviderID == "" || msg.ModelID == "" {
			continue
		}
		cfg := &ModelConfig{
			ProviderID:  msg.ProviderID,
			ModelID:     msg.ModelID,
			LastUpdated: time.Now(),
		}
		c.sessionModel.Store(sessionID, cfg)
		return cfg, true
	}

	c.defaultModelMu.RLock()
	defer c.defaultModelMu.RUnlock()
	if c.defaultModel != nil {
		return c.defaultModel, true
	}
	return nil, false
}

func hasAttachmentType(att Attachment, prefix string) bool {
	m := strings.ToLower(strings.TrimSpace(att.Mime))
	return strings.HasPrefix(m, prefix)
}

func capabilitySupportsModality(cap *ModelCapability, modality string) bool {
	if cap == nil {
		return false
	}
	modality = strings.ToLower(strings.TrimSpace(modality))
	if modality == "" {
		return false
	}
	if cap.InputModalities[modality] {
		return true
	}
	if modality == "text" {
		return cap.InputModalities["input_text"] || cap.InputModalities["prompt"]
	}
	if modality == "image" {
		return cap.InputModalities["vision"] || cap.InputModalities["input_image"]
	}
	if modality == "video" {
		return cap.InputModalities["input_video"]
	}
	if modality == "audio" {
		return cap.InputModalities["voice"] || cap.InputModalities["speech"] || cap.InputModalities["input_audio"]
	}
	return false
}

func maybeVisionCapableByModelID(modelID string) bool {
	id := strings.ToLower(strings.TrimSpace(modelID))
	if id == "" {
		return false
	}
	for _, hint := range []string{"kimi", "gpt-4o", "gemini", "qwen-vl", "qvq", "claude-3-5-sonnet", "claude-3-7-sonnet", "vision", "vl"} {
		if strings.Contains(id, hint) {
			return true
		}
	}
	return false
}

func (c *Client) preprocessAttachmentsForSession(ctx context.Context, sessionID string, payload *MessagePayload, effectiveContent *string) (bool, error) {
	if payload == nil || len(payload.Attachments) == 0 {
		return false, nil
	}
	c.mediaDebugf("preprocess start: session=%s attachments=%d", sessionID[:min(8, len(sessionID))], len(payload.Attachments))

	needImage := false
	needVideo := false
	needAudio := false
	mediaAttachments := make([]Attachment, 0)
	for _, att := range payload.Attachments {
		if hasAttachmentType(att, "image/") {
			needImage = true
			mediaAttachments = append(mediaAttachments, att)
			continue
		}
		if hasAttachmentType(att, "video/") {
			needVideo = true
			mediaAttachments = append(mediaAttachments, att)
			continue
		}
		if hasAttachmentType(att, "audio/") {
			needAudio = true
			mediaAttachments = append(mediaAttachments, att)
		}
	}

	if !needImage && !needVideo && !needAudio {
		c.mediaDebugf("no image/video/audio attachments, skip preprocess")
		return false, nil
	}
	c.mediaDebugf("media detected: needImage=%t needVideo=%t needAudio=%t mediaCount=%d", needImage, needVideo, needAudio, len(mediaAttachments))

	c.ensureModelCatalog(ctx)
	recognizerModel, ok := c.selectFallbackMediaModel(needImage, needVideo, needAudio)
	if !ok {
		c.mediaDebugf("no matched recognizer model for media, skip preprocessing")
		return false, nil
	}
	c.mediaDebugf("matched recognizer model: %s/%s", recognizerModel.ProviderID, recognizerModel.ModelID)

	recognized, err := c.recognizeMediaWithModel(ctx, mediaAttachments, recognizerModel)
	if err != nil {
		c.mediaDebugf("media recognizer failed: %v", err)
		return false, err
	}
	if strings.TrimSpace(recognized) == "" {
		c.mediaDebugf("media recognizer returned empty text, keep original flow")
		return false, nil
	}

	*effectiveContent = fmt.Sprintf("[澶氭ā鎬侀澶勭悊缁撴灉]\n%s\n\n[鐢ㄦ埛璇锋眰]\n%s", recognized, *effectiveContent)

	filtered := make([]Attachment, 0, len(payload.Attachments))
	for _, att := range payload.Attachments {
		if hasAttachmentType(att, "image/") || hasAttachmentType(att, "video/") || hasAttachmentType(att, "audio/") {
			continue
		}
		filtered = append(filtered, att)
	}
	payload.Attachments = filtered
	c.mediaDebugf("preprocess done: converted media to text, remaining attachments=%d", len(filtered))

	return true, nil
}

func (c *Client) selectFallbackMediaModel(needImage, needVideo, needAudio bool) (*ModelConfig, bool) {
	c.modelCatalogMu.RLock()
	keys := make([]string, 0, len(c.modelCatalog))
	for k := range c.modelCatalog {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Pass 1: strict capability matching based on provider metadata.
	for _, k := range keys {
		cap := c.modelCatalog[k]
		if cap == nil {
			continue
		}
		if !capabilitySupportsModality(cap, "text") {
			continue
		}
		if needImage && !capabilitySupportsModality(cap, "image") {
			continue
		}
		if needVideo && !capabilitySupportsModality(cap, "video") {
			continue
		}
		if needAudio && !capabilitySupportsModality(cap, "audio") {
			continue
		}
		cfg := &ModelConfig{ProviderID: cap.ProviderID, ModelID: cap.ModelID}
		c.mediaDebugf("selected media recognizer model: %s/%s", cfg.ProviderID, cfg.ModelID)
		c.modelCatalogMu.RUnlock()
		return cfg, true
	}

	// Pass 2: metadata-missing fallback (common in some OpenAI-compatible providers).
	// For image/video recognition, prefer known vision-capable model IDs (e.g., Kimi).
	if (needImage || needVideo) && !needAudio {
		for _, k := range keys {
			cap := c.modelCatalog[k]
			if cap == nil {
				continue
			}
			if maybeVisionCapableByModelID(cap.ModelID) {
				cfg := &ModelConfig{ProviderID: cap.ProviderID, ModelID: cap.ModelID}
				c.mediaDebugf("selected media recognizer model by heuristic: %s/%s", cfg.ProviderID, cfg.ModelID)
				c.modelCatalogMu.RUnlock()
				return cfg, true
			}
		}
	}

	c.modelCatalogMu.RUnlock()
	c.mediaDebugf("no media recognizer model found for needImage=%t needVideo=%t needAudio=%t", needImage, needVideo, needAudio)
	return nil, false
}

func (c *Client) recognizeMediaWithModel(ctx context.Context, attachments []Attachment, visionModel *ModelConfig) (string, error) {
	if visionModel == nil {
		return "", fmt.Errorf("media recognizer model is nil")
	}
	c.mediaDebugf("recognize with fallback model: %s/%s attachments=%d", visionModel.ProviderID, visionModel.ModelID, len(attachments))

	prepCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	tmpSession, err := c.sdk.Session.New(prepCtx, opencode.SessionNewParams{
		Title: opencode.F("[media-preprocess]"),
	})
	if err != nil {
		return "", fmt.Errorf("create media preprocess session: %w", err)
	}
	c.mediaDebugf("media preprocess temp session created: %s", tmpSession.ID[:min(8, len(tmpSession.ID))])
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, delErr := c.sdk.Session.Delete(cleanupCtx, tmpSession.ID, opencode.SessionDeleteParams{}); delErr != nil {
			log.Printf("opencode: cleanup media preprocess session %s failed: %v", tmpSession.ID[:min(8, len(tmpSession.ID))], delErr)
		}
	}()

	parts := []opencode.SessionPromptParamsPartUnion{
		opencode.TextPartInputParam{
			Type: opencode.F(opencode.TextPartInputTypeText),
			Text: opencode.F("Analyze attached media (image/video/audio). For audio, transcribe key content. Return a concise summary focusing on information useful to answer user questions."),
		},
	}

	for _, att := range attachments {
		if att.URL == "" || att.Mime == "" {
			continue
		}
		parts = append(parts, opencode.FilePartInputParam{
			Type: opencode.F(opencode.FilePartInputTypeFile),
			Mime: opencode.F(att.Mime),
			URL:  opencode.F(att.URL),
		})
	}

	modelOverride := &opencode.SessionPromptParamsModel{
		ProviderID: opencode.F(visionModel.ProviderID),
		ModelID:    opencode.F(visionModel.ModelID),
	}

	resp, err := c.sendPromptWithRetry(prepCtx, tmpSession.ID, parts, modelOverride)
	if err != nil {
		return "", fmt.Errorf("media recognize with %s/%s: %w", visionModel.ProviderID, visionModel.ModelID, err)
	}
	recognized := strings.TrimSpace(extractReplyFromMessage(resp))
	c.mediaDebugf("media recognize completed: textLen=%d", len(recognized))

	return recognized, nil
}

// sendPromptAsync 璋冪敤 OpenCode 鐨?prompt_async 鎺ュ彛锛岀珛鍗宠繑鍥烇紝鐢变簨浠舵祦鎻愪緵缁撴灉
func (c *Client) sendPromptAsync(ctx context.Context, sessionID string, parts []opencode.SessionPromptParamsPartUnion, model *opencode.SessionPromptParamsModel) error {
	if c.endpoint == "" {
		return fmt.Errorf("opencode: prompt_async unavailable: missing endpoint")
	}

	params := opencode.SessionPromptParams{
		Parts: opencode.F(parts),
	}
	if model != nil {
		params.Model = opencode.F(*model)
	}
	if c.directory != "" {
		params.Directory = opencode.F(c.directory)
	}

	body, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("opencode: prompt_async marshal: %w", err)
	}

	promptURL, err := url.JoinPath(c.endpoint, "session", sessionID, "prompt_async")
	if err != nil {
		promptURL = fmt.Sprintf("%s/session/%s/prompt_async", strings.TrimRight(c.endpoint, "/"), sessionID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, promptURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("opencode: prompt_async request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("opencode: prompt_async do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("opencode: prompt_async unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	log.Printf("opencode: prompt_async accepted for session %s (status=%d)", sessionID[:8], resp.StatusCode)
	return nil
}

// ========== 璇锋眰鍘婚噸 ==========

// generateRequestHash 鐢熸垚璇锋眰鐨勫敮涓€hash
func generateRequestHash(payload MessagePayload) string {
	data := fmt.Sprintf("%s|%s|%s|%s", payload.Channel, payload.UserID, payload.ThreadID, payload.Content)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:16]) // 浣跨敤鍓?6瀛楄妭
}

// checkAndMarkRequest 妫€鏌ヨ姹傛槸鍚﹂噸澶嶏紝濡傛灉涓嶉噸澶嶅垯鏍囪涓哄鐞嗕腑
func (c *Client) checkAndMarkRequest(hash string) (*RequestRecord, bool) {
	now := time.Now()

	// 妫€鏌ユ槸鍚﹀瓨鍦?
	if val, ok := c.requestCache.Load(hash); ok {
		record := val.(*RequestRecord)
		// 妫€鏌ユ槸鍚﹀湪鏃堕棿绐楀彛鍐?
		if now.Sub(record.Timestamp) < RequestDeduplicationWindow {
			if record.InFlight {
				// 姝ｅ湪澶勭悊涓紝杩斿洖閲嶅锛堢湡姝ｇ殑閲嶅璇锋眰锛?
				log.Printf("opencode: duplicate request detected (in-flight), age: %v", now.Sub(record.Timestamp))
				return record, true
			}
			// 宸插畬鎴愮殑璇锋眰锛屼笉璁や负鏄噸澶嶏紙鍏佽鐢ㄦ埛鍐嶆鍙戦€佺浉鍚屾秷鎭級
			// 鍙繑鍥炵紦瀛樼殑鍝嶅簲浠ュ姞蹇搷搴旈€熷害
			log.Printf("opencode: returning cached response (request completed %v ago)", now.Sub(record.Timestamp))
			return record, true
		}
		// 瓒呭嚭鏃堕棿绐楀彛锛屽彲浠ラ噸鏂板鐞?
	}

	// 鏍囪涓哄鐞嗕腑
	record := &RequestRecord{
		Hash:      hash,
		Timestamp: now,
		InFlight:  true,
	}
	c.requestCache.Store(hash, record)
	return record, false
}

// completeRequest 瀹屾垚璇锋眰骞剁紦瀛樼粨鏋?
func (c *Client) completeRequest(hash string, response Response) {
	if val, ok := c.requestCache.Load(hash); ok {
		record := val.(*RequestRecord)
		record.Response = response
		record.InFlight = false
		record.Timestamp = time.Now() // 鏇存柊鏃堕棿鎴?
		c.requestCache.Store(hash, record)
	}
}

// failRequest 鏍囪璇锋眰澶辫触
func (c *Client) failRequest(hash string) {
	c.requestCache.Delete(hash)
}

// StorePendingQuestion stores a question for later answering
func (c *Client) StorePendingQuestion(q *Question) {
	c.pendingQuestions.Store(q.ID, q)
}

// GetPendingQuestion retrieves a pending question
func (c *Client) GetPendingQuestion(questionID string) (*Question, bool) {
	val, ok := c.pendingQuestions.Load(questionID)
	if !ok {
		return nil, false
	}
	return val.(*Question), true
}

// GetLatestPendingPermission 鑾峰彇鎸囧畾 session 鏈€杩戠殑寰呭鐞嗘潈闄愯姹?
// 濡傛灉 sessionID 涓虹┖锛岃繑鍥炰换鎰忔渶杩戠殑鏉冮檺璇锋眰
func (c *Client) GetLatestPendingPermission(sessionID string) (*Question, bool) {
	var latest *Question
	c.pendingQuestions.Range(func(key, value interface{}) bool {
		q := value.(*Question)
		// 鍙繑鍥炴潈闄愯姹傦紙浠?per_ 寮€澶达級
		if !strings.HasPrefix(q.ID, "per_") {
			return true
		}
		// 濡傛灉鎸囧畾浜?sessionID锛屽彧杩斿洖璇?session 鐨?
		if sessionID != "" && q.SessionID != sessionID {
			return true
		}
		// 鎵炬渶杩戝垱寤虹殑
		if latest == nil || q.CreatedAt.After(latest.CreatedAt) {
			latest = q
		}
		return true
	})
	if latest != nil {
		return latest, true
	}
	return nil, false
}

// GetLatestPendingQuestion 鑾峰彇鎸囧畾 session 鏈€杩戠殑寰呭鐞嗛棶棰橈紙闈炴潈闄愯姹傦級
// 濡傛灉 sessionID 涓虹┖锛岃繑鍥炰换鎰忔渶杩戠殑闂
func (c *Client) GetLatestPendingQuestion(sessionID string) (*Question, bool) {
	var latest *Question
	c.pendingQuestions.Range(func(key, value interface{}) bool {
		q := value.(*Question)
		// 鎺掗櫎鏉冮檺璇锋眰锛堜互 per_ 寮€澶达級
		if strings.HasPrefix(q.ID, "per_") {
			return true
		}
		// 濡傛灉鎸囧畾浜?sessionID锛屽彧杩斿洖璇?session 鐨?
		if sessionID != "" && q.SessionID != sessionID {
			return true
		}
		// 鎵炬渶杩戝垱寤虹殑
		if latest == nil || q.CreatedAt.After(latest.CreatedAt) {
			latest = q
		}
		return true
	})
	if latest != nil {
		return latest, true
	}
	return nil, false
}

// DeletePendingQuestion removes a pending question
func (c *Client) DeletePendingQuestion(questionID string) {
	c.pendingQuestions.Delete(questionID)
}

// AnswerQuestion submits an answer to a pending question or permission request
func (c *Client) AnswerQuestion(ctx context.Context, questionID string, answer string) error {
	q, ok := c.GetPendingQuestion(questionID)
	if !ok {
		return fmt.Errorf("question not found: %s", questionID)
	}

	if c.endpoint == "" {
		return fmt.Errorf("opencode: answer question unavailable: missing endpoint")
	}

	// 鍒ゆ柇鏄潈闄愯姹傝繕鏄櫘閫氶棶棰?
	if strings.HasPrefix(questionID, "per_") {
		return c.answerPermission(ctx, q, answer)
	}

	return c.answerNormalQuestion(ctx, q, answer)
}

// answerPermission answers a permission request (internal, via AnswerQuestion path).
func (c *Client) answerPermission(ctx context.Context, q *Question, answer string) error {
_, responseStr, ok := parsePermissionAnswer(answer)
if !ok {
return fmt.Errorf("invalid permission answer (raw=%q bytes=% X)", answer, []byte(answer))
}
log.Printf("opencode: answerPermission via parsePermissionAnswer - ID=%s, responseStr=%s", q.ID, responseStr)
return c.RespondToPermission(ctx, q.ID, responseStr)
}

func parsePermissionAnswer(answer string) (opencode.SessionPermissionRespondParamsResponse, string, bool) {
	normalized := normalizePermissionAnswer(answer)
	if normalized == "" {
		return "", "", false
	}

	allowTokens := []string{"1", "allow", "yes", "鍏佽", "鍚屾剰", "纭", "ok", "okay", "y", "鍙互", "琛"}
	rejectTokens := []string{"2", "deny", "no", "鎷掔粷", "涓嶅悓鎰", "鍙栨秷", "n"}
	alwaysTokens := []string{"3", "always", "濮嬬粓鍏佽", "濮嬬粓", "涓€鐩村厑璁", "鎬绘槸鍏佽"}

	if containsAnyToken(normalized, alwaysTokens) {
		return opencode.SessionPermissionRespondParamsResponseAlways, "always", true
	}
	if containsAnyToken(normalized, rejectTokens) {
		return opencode.SessionPermissionRespondParamsResponseReject, "reject", true
	}
	if containsAnyToken(normalized, allowTokens) {
		return opencode.SessionPermissionRespondParamsResponseOnce, "once", true
	}

	// 鍏滃簳锛氬厛鍒ゆ柇鏄庣‘鍚﹀畾锛屽啀鍒ゆ柇鍏佽锛岄伩鍏嶁€滀笉鍏佽鈥濊璇垽涓哄厑璁?
	if strings.Contains(normalized, "涓嶅厑璁") || strings.Contains(normalized, "鎷掔粷") || strings.Contains(normalized, "涓嶅悓鎰") {
		return opencode.SessionPermissionRespondParamsResponseReject, "reject", true
	}
	if strings.Contains(normalized, "濮嬬粓") || strings.Contains(normalized, "always") {
		return opencode.SessionPermissionRespondParamsResponseAlways, "always", true
	}
	if strings.Contains(normalized, "鍏佽") || strings.Contains(normalized, "鍚屾剰") || strings.Contains(normalized, "纭") {
		return opencode.SessionPermissionRespondParamsResponseOnce, "once", true
	}

	return "", "", false
}

// normalizePermissionAnswer 鏍囧噯鍖栧洖澶嶆枃鏈細杞皬鍐欏苟绉婚櫎绌烘牸銆佹爣鐐广€佺鍙?
func normalizePermissionAnswer(answer string) string {
	answerLower := strings.TrimSpace(strings.ToLower(answer))
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r) {
			return -1
		}
		return r
	}, answerLower)
}

func containsAnyToken(text string, tokens []string) bool {
	for _, token := range tokens {
		if text == token || strings.Contains(text, token) {
			return true
		}
	}
	return false
}

// RespondToPermission answers a permission request using a canonical English response.
// response must be "once" (allow this time), "reject" (deny), or "always" (always allow).
// Adapters should resolve locale-specific text to one of these values before calling.
func (c *Client) RespondToPermission(ctx context.Context, permissionID, response string) error {
switch response {
case "once", "reject", "always":
default:
return fmt.Errorf("invalid permission response %q: must be once/reject/always", response)
}

q, ok := c.GetPendingQuestion(permissionID)
if !ok {
return fmt.Errorf("permission not found: %s", permissionID)
}

directory := q.Directory
if directory == "" {
directory = c.directory
}

log.Printf("opencode: RespondToPermission - ID=%s, sessionID=%s, response=%s", permissionID, q.SessionID, response)

if err := c.answerPermissionViaHTTP(ctx, q, response); err != nil {
log.Printf("opencode: HTTP permission API failed: %v, falling back to SDK", err)

var responseParam opencode.SessionPermissionRespondParamsResponse
switch response {
case "always":
responseParam = opencode.SessionPermissionRespondParamsResponseAlways
case "reject":
responseParam = opencode.SessionPermissionRespondParamsResponseReject
default:
responseParam = opencode.SessionPermissionRespondParamsResponseOnce
}
result, sdkErr := c.sdk.Session.Permissions.Respond(ctx, q.SessionID, permissionID,
opencode.SessionPermissionRespondParams{
Response:  opencode.F(responseParam),
Directory: opencode.F(directory),
})
if sdkErr != nil {
return fmt.Errorf("permission respond failed (HTTP: %v, SDK: %w)", err, sdkErr)
}
if result != nil {
log.Printf("opencode: SDK permission respond succeeded")
}
}

c.DeletePendingQuestion(permissionID)
log.Printf("opencode: permission %s answered (%s) for session %s", permissionID, response, q.SessionID[:8])
return nil
}
// answerPermissionViaHTTP 鐩存帴璋冪敤 HTTP API锛堜笌 Python 鐗堟湰涓€鑷达級
func (c *Client) answerPermissionViaHTTP(ctx context.Context, q *Question, response string) error {
	if c.endpoint == "" {
		return fmt.Errorf("opencode: answer permission via HTTP unavailable: missing endpoint")
	}

	// 鏋勯€?URL锛歅OST /session/{sessionID}/permissions/{permissionID}
	permissionURL := fmt.Sprintf("%s/session/%s/permissions/%s", c.endpoint, q.SessionID, q.ID)

	payload := map[string]interface{}{
		"response": response,
	}
	// 浣跨敤 Question 涓繚瀛樼殑 directory锛屽鏋滀负绌哄垯浣跨敤 client 鐨勯粯璁?directory
	directory := q.Directory
	if directory == "" {
		directory = c.directory
	}
	// 鈿狅笍 娉ㄦ剰锛氬彧鍦?payload 涓彂閫?directory锛屼笉瑕佸湪 query string 涓噸澶?
	if directory != "" {
		payload["directory"] = directory
	}

	log.Printf("opencode: HTTP permission API - URL=%s, payload=%+v", permissionURL, payload)

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("opencode: permission HTTP marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, permissionURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("opencode: permission HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuthHeaders(req.Header)
	// 涓嶅湪 query string 涓彂閫?directory锛孭ython 涔熸病鏈夎繖鏍峰仛

	log.Printf("opencode: HTTP permission request - URL=%s, method=POST, body=%s",
		req.URL.String(), string(body))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("opencode: permission HTTP do: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("opencode: HTTP permission response - status=%d", resp.StatusCode)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("opencode: permission HTTP unexpected status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	c.DeletePendingQuestion(q.ID)
	log.Printf("opencode: answered permission %s for session %s via HTTP API", q.ID, q.SessionID[:8])
	return nil
}

// answerNormalQuestion 鍥炵瓟鏅€氶棶棰?
// API 绔偣: POST /question/:requestID/reply with body {"answers": [[绛旀1], [绛旀2], ...]}
// answers 鏄竴涓簩缁存暟缁勶細姣忎釜瀛愭暟缁勫搴斾竴涓?question 鐨勭瓟妗堬紙澶氶€夊彲浠ユ湁澶氫釜鍏冪礌锛?
//
// 绛旀鏍煎紡鏀寔:
// - 绠€鍗曟牸寮? "1" - 涓虹涓€涓棶棰橀€夋嫨閫夐」1
// - 澶氶棶棰樻牸寮? "1;2,3;1" - 鐢ㄥ垎鍙峰垎闅斾笉鍚岄棶棰樼殑绛旀锛岀敤閫楀彿鍒嗛殧澶氶€夌瓟妗?
// - 鏍囩鏍煎紡: "绾疕TML椤甸潰;API鎺ュ彛璇锋眰;GPU鍒╃敤鐜?鏄惧瓨浣跨敤鎯呭喌"
func (c *Client) answerNormalQuestion(ctx context.Context, q *Question, answer string) error {
	// 鏍规嵁闂 ID 绫诲瀷閫夋嫨涓嶅悓鐨勫洖绛旀柟寮?
	// que_xxx: 浣跨敤 /question/:id/reply 绔偣
	// 鍏朵粬: 浣跨敤 /session/:id/message/:messageID/answer 绔偣

	var answerURL string
	var payload map[string]interface{}

	if strings.HasPrefix(q.ID, "que_") {
		// 鏂扮増闂鏍煎紡锛屼娇鐢?/question/:id/reply 绔偣
		answerURL = fmt.Sprintf("%s/question/%s/reply", c.endpoint, q.ID)

		// 瑙ｆ瀽绛旀
		var allAnswers [][]string

		// 妫€鏌ユ槸鍚︿娇鐢ㄥ垎鍙峰垎闅斿涓棶棰樼殑绛旀
		if strings.Contains(answer, ";") {
			// 澶氶棶棰樻牸寮? "1;2,3;1"
			questionAnswers := strings.Split(answer, ";")
			for idx, qa := range questionAnswers {
				var answerItems []string
				if strings.Contains(qa, ",") {
					// 澶氶€夌瓟妗?
					for _, item := range strings.Split(qa, ",") {
						if trimmed := strings.TrimSpace(item); trimmed != "" {
							answerItems = append(answerItems, c.resolveAnswerOption(q, idx, trimmed))
						}
					}
				} else {
					// 鍗曢€夌瓟妗?
					if trimmed := strings.TrimSpace(qa); trimmed != "" {
						answerItems = []string{c.resolveAnswerOption(q, idx, trimmed)}
					}
				}
				if len(answerItems) > 0 {
					allAnswers = append(allAnswers, answerItems)
				}
			}
		} else if strings.Contains(answer, ",") {
			// 鍗曢棶棰樺閫夋牸寮? "閫夐」1,閫夐」2"
			var answerItems []string
			for _, item := range strings.Split(answer, ",") {
				if trimmed := strings.TrimSpace(item); trimmed != "" {
					answerItems = append(answerItems, c.resolveAnswerOption(q, 0, trimmed))
				}
			}
			allAnswers = [][]string{answerItems}
		} else {
			// 鍗曢棶棰樺崟閫夋牸寮? "1" 鎴?"閫夐」1"
			resolved := c.resolveAnswerOption(q, 0, strings.TrimSpace(answer))
			allAnswers = [][]string{{resolved}}
		}

		payload = map[string]interface{}{
			"answers": allAnswers,
		}
		log.Printf("opencode: answering question %s via /question/reply endpoint, answers=%v", q.ID, allAnswers)
	} else {
		// 鏃х増鏍煎紡锛屼娇鐢?/session/:id/message/:messageID/answer 绔偣
		answerURL = fmt.Sprintf("%s/session/%s/message/%s/answer", c.endpoint, q.SessionID, q.MessageID)
		payload = map[string]interface{}{
			"answer": answer,
		}
		log.Printf("opencode: answering question %s via /session/message/answer endpoint", q.ID)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("opencode: answer marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, answerURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("opencode: answer request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("opencode: answer do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("opencode: answer unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	c.DeletePendingQuestion(q.ID)
	log.Printf("opencode: answered question %s for session %s", q.ID, q.SessionID[:8])
	return nil
}

// resolveAnswerOption 灏嗙敤鎴疯緭鍏ョ殑绛旀锛堝彲鑳芥槸鏁板瓧绱㈠紩锛夎В鏋愪负瀹為檯閫夐」鏍囩
// questionIndex: 绗嚑涓瓙闂 (0-based)
// input: 鐢ㄦ埛杈撳叆锛屽彲鑳芥槸 "1" 鎴?"绾疕TML椤甸潰" 绛?
func (c *Client) resolveAnswerOption(q *Question, questionIndex int, input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}

	log.Printf("opencode: resolveAnswerOption - qID: %s, questionIndex: %d, input: '%s', hasQuestions: %d, hasSimpleOptions: %d",
		q.ID, questionIndex, input, len(q.Questions), len(q.Options))

	// 浼樺厛浣跨敤鏂扮増 Questions 鏁扮粍
	if len(q.Questions) > 0 {
		if questionIndex >= len(q.Questions) {
			log.Printf("opencode: questionIndex %d out of range (has %d questions), returning original input", questionIndex, len(q.Questions))
			return input
		}

		qi := q.Questions[questionIndex]

		// 灏濊瘯灏嗚緭鍏ヨВ鏋愪负鏁板瓧
		if idx, err := strconv.Atoi(input); err == nil {
			// 鏁板瓧绱㈠紩鏄?1-based
			if idx >= 1 && idx <= len(qi.Options) {
				result := qi.Options[idx-1].Label
				log.Printf("opencode: converted number %d -> '%s'", idx, result)
				return result
			} else {
				log.Printf("opencode: number %d out of range (1-%d)", idx, len(qi.Options))
			}
		}

		// 濡傛灉涓嶆槸鏈夋晥鐨勬暟瀛楋紝妫€鏌ユ槸鍚︽槸鏈夋晥鐨勯€夐」鏍囩
		for _, opt := range qi.Options {
			if strings.EqualFold(opt.Label, input) {
				log.Printf("opencode: matched option label '%s'", opt.Label)
				return opt.Label
			}
		}
	}

	// 鍥為€€鍒扮畝鍖?Options 鏁扮粍锛堟棫鏍煎紡鍏煎锛?
	if len(q.Options) > 0 {
		// 濡傛灉 questionIndex 涓?0锛屽皾璇曚娇鐢ㄧ畝鍖栭€夐」
		if questionIndex == 0 {
			if idx, err := strconv.Atoi(input); err == nil {
				if idx >= 1 && idx <= len(q.Options) {
					result := q.Options[idx-1]
					log.Printf("opencode: converted number %d -> '%s' (simple options)", idx, result)
					return result
				}
			}
			for _, opt := range q.Options {
				if strings.EqualFold(opt, input) {
					log.Printf("opencode: matched simple option '%s'", opt)
					return opt
				}
			}
		}
	}

	// 鏃犳硶瑙ｆ瀽锛岃繑鍥炲師濮嬭緭鍏?
	log.Printf("opencode: could not resolve input '%s', returning original", input)
	return input
}

// cleanupRequestCache 瀹氭湡娓呯悊杩囨湡鐨勮姹傜紦瀛?
func (c *Client) cleanupRequestCache() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		c.requestCache.Range(func(key, value interface{}) bool {
			record := value.(*RequestRecord)
			if now.Sub(record.Timestamp) > RequestDeduplicationWindow {
				c.requestCache.Delete(key)
			}
			return true
		})
	}
}

// ========== Skill鎻愮ず ==========

// refreshSkillCache 鍒锋柊鍙敤skill缂撳瓨
func (c *Client) refreshSkillCache(ctx context.Context) {
	agents, err := c.ListAgents(ctx)
	if err != nil {
		log.Printf("opencode: failed to refresh skill cache: %v", err)
		return
	}

	c.skillCacheMu.Lock()
	defer c.skillCacheMu.Unlock()

	c.skillHintCache = make([]string, 0, len(agents))
	for _, agent := range agents {
		hint := agent.Name
		if agent.Description != "" {
			hint = fmt.Sprintf("%s (%s)", agent.Name, agent.Description)
		}
		c.skillHintCache = append(c.skillHintCache, hint)
	}

	log.Printf("opencode: refreshed skill cache with %d skills", len(c.skillHintCache))
}

// getSkillHint 鑾峰彇skill鎻愮ず鏂囨湰
func (c *Client) getSkillHint() string {
	c.skillCacheMu.RLock()
	defer c.skillCacheMu.RUnlock()

	if len(c.skillHintCache) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("\n\n[鍙敤鎶€鑳芥彁绀篯 濡傛灉闇€瑕侊紝浣犲彲浠ヤ娇鐢ㄤ互涓嬫妧鑳斤細")
	for _, skill := range c.skillHintCache {
		sb.WriteString("\n- ")
		sb.WriteString(skill)
	}
	sb.WriteString("\\n璇峰湪闇€瑕佹椂涓诲姩璋冪敤杩欎簺鎶€鑳姐€?")

	return sb.String()
}

// enhanceContentWithSkillHint 鍦ㄦ秷鎭唴瀹逛腑娣诲姞skill鎻愮ず
func (c *Client) enhanceContentWithSkillHint(content string, sessionID string) string {
	if !c.enableSkillHint {
		return content
	}

	// 妫€鏌ユ槸鍚﹂渶瑕佸埛鏂扮紦瀛?
	c.skillCacheMu.RLock()
	needRefresh := len(c.skillHintCache) == 0
	c.skillCacheMu.RUnlock()

	if needRefresh {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		c.refreshSkillCache(ctx)
		cancel()
	}

	hint := c.getSkillHint()
	if hint == "" {
		return content
	}

	// 鍙湪session鐨勫墠鍑犳潯娑堟伅娣诲姞鎻愮ず锛岄伩鍏嶅啑浣?
	msgCount := c.GetMessageCount(sessionID)
	if msgCount > 3 {
		return content
	}

	return content + hint
}

// RefreshSkills 鎵嬪姩鍒锋柊skill缂撳瓨
func (c *Client) RefreshSkills(ctx context.Context) error {
	c.refreshSkillCache(ctx)
	return nil
}

// SetSkillHintEnabled 璁剧疆鏄惁鍚敤skill鎻愮ず
func (c *Client) SetSkillHintEnabled(enabled bool) {
	c.enableSkillHint = enabled
}









