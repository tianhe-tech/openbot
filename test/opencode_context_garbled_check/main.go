package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	clientpkg "github.com/user/opencode-gateway/internal/opencode"
)

type phaseResult struct {
	PhaseTarget  float64
	Turn         int
	TokenCount   int
	ContextLen   int
	Usage        float64
	ReplyLen     int
	StreamChunks int
	Garbled      bool
	Reason       string
	Err          error
}

func main() {
	endpointFlag := flag.String("endpoint", "", "OpenCode endpoint, default from OPENCODE_ENDPOINT or http://127.0.0.1:4096")
	apiKeyFlag := flag.String("api-key", "", "OpenCode API key, default from OPENCODE_API_KEY")
	mode := flag.String("mode", "sync", "Run mode: sync or stream")
	channel := flag.String("channel", "context-check", "Logical channel name used for session mapping")
	userID := flag.String("user", "garbled-check-user", "Logical user id")
	threadPrefix := flag.String("thread-prefix", "garbled-check", "Thread id prefix")
	maxTurns := flag.Int("max-turns", 36, "Maximum conversation turns to run")
	timeoutSec := flag.Int("timeout-sec", 180, "Per-turn timeout in seconds")
	repeatBase := flag.Int("repeat-base", 10, "Base repetition count for each turn payload")
	repeatJitter := flag.Int("repeat-jitter", 8, "Turn-based repetition jitter")
	minStreamChunks := flag.Int("min-stream-chunks", 1, "Minimum streamed text chunks required per turn in stream mode")
	continueOnGarbled := flag.Bool("continue-on-garbled", false, "Continue running even after a garbled response is detected")
	flag.Parse()

	endpoint := strings.TrimSpace(*endpointFlag)
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("OPENCODE_ENDPOINT"))
	}
	if endpoint == "" {
		endpoint = "http://127.0.0.1:4096"
	}

	apiKey := strings.TrimSpace(*apiKeyFlag)
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("OPENCODE_API_KEY"))
	}

	threadID := fmt.Sprintf("%s-%d", strings.TrimSpace(*threadPrefix), time.Now().Unix())
	client := clientpkg.NewClient(endpoint, apiKey, clientpkg.WithDirectory("."))

	fmt.Println("OpenCode long-context garbled response check")
	fmt.Println("=========================================")
	fmt.Printf("Endpoint: %s\n", endpoint)
	fmt.Printf("Mode: %s\n", strings.ToLower(strings.TrimSpace(*mode)))
	fmt.Printf("Channel/User/Thread: %s / %s / %s\n", *channel, *userID, threadID)
	fmt.Printf("Max turns: %d\n", *maxTurns)
	fmt.Printf("Payload repeat base/jitter: %d / %d\n", *repeatBase, *repeatJitter)
	fmt.Printf("Min stream chunks: %d\n", *minStreamChunks)
	fmt.Println()

	healthCtx, healthCancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := client.CheckHealth(healthCtx)
	healthCancel()
	if err != nil {
		fmt.Printf("Health check failed: %v\n", err)
		os.Exit(1)
	}

	phases := []float64{0.70, 0.85, 0.95, 1.05}
	results := make([]phaseResult, 0, *maxTurns)
	turn := 0
	sessionID := ""

	if strings.EqualFold(strings.TrimSpace(*mode), "stream") {
		listenerCtx, listenerCancel := context.WithCancel(context.Background())
		defer listenerCancel()
		if err := client.StartEventListener(listenerCtx); err != nil {
			fmt.Printf("StartEventListener failed: %v\n", err)
			os.Exit(1)
		}
		// Give the listener a brief moment to connect before first stream prompt.
		time.Sleep(400 * time.Millisecond)
	}

	for _, phase := range phases {
		for {
			if turn >= *maxTurns {
				break
			}

			turn++
			msg := buildTurnMessage(turn, *repeatBase, *repeatJitter)
			msgTokens := estimateTokens(msg)

			reqCtx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
			resp, streamChunks, sendErr := sendByMode(reqCtx, client, strings.ToLower(strings.TrimSpace(*mode)), *minStreamChunks, clientpkg.MessagePayload{
				Channel:   *channel,
				UserID:    *userID,
				ThreadID:  threadID,
				Content:   msg,
				Streaming: strings.EqualFold(strings.TrimSpace(*mode), "stream"),
			})
			cancel()

			if sendErr != nil {
				results = append(results, phaseResult{PhaseTarget: phase, Turn: turn, Err: sendErr})
				fmt.Printf("[turn %02d] send failed: %v\n", turn, sendErr)
				break
			}

			if sessionID == "" {
				sessionID = resp.SessionID
			}

			infoCtx, infoCancel := context.WithTimeout(context.Background(), 10*time.Second)
			info, infoErr := client.GetSessionInfo(infoCtx, sessionID)
			infoCancel()

			if infoErr != nil {
				results = append(results, phaseResult{PhaseTarget: phase, Turn: turn, Err: infoErr})
				fmt.Printf("[turn %02d] get session info failed: %v\n", turn, infoErr)
				break
			}

			garbled, reason := isGarbled(resp.Reply)
			usage := info.ContextUsage

			results = append(results, phaseResult{
				PhaseTarget:  phase,
				Turn:         turn,
				TokenCount:   info.TokenCount,
				ContextLen:   info.ContextLength,
				Usage:        usage,
				ReplyLen:     len([]rune(resp.Reply)),
				StreamChunks: streamChunks,
				Garbled:      garbled,
				Reason:       reason,
			})

			fmt.Printf("[turn %02d] phase=%.0f%% msg_tokens=%d usage=%.1f%% reply_len=%d",
				turn,
				phase*100,
				msgTokens,
				usage*100,
				len([]rune(resp.Reply)),
			)
			if strings.EqualFold(strings.TrimSpace(*mode), "stream") {
				fmt.Printf(" stream_chunks=%d", streamChunks)
			}
			fmt.Printf(" garbled=%v", garbled)
			if reason != "" {
				fmt.Printf(" reason=%s", reason)
			}
			fmt.Println()

			if garbled && !*continueOnGarbled {
				printSummary(results)
				os.Exit(2)
			}

			if usage >= phase {
				break
			}
		}

		if turn >= *maxTurns {
			break
		}
	}

	printSummary(results)

	if hasErrors(results) {
		os.Exit(3)
	}

	if hasGarbled(results) {
		os.Exit(2)
	}
}

func sendByMode(ctx context.Context, client *clientpkg.Client, mode string, minStreamChunks int, payload clientpkg.MessagePayload) (clientpkg.Response, int, error) {
	if mode != "stream" {
		resp, err := client.SendMessage(ctx, payload)
		return resp, 0, err
	}

	var chunks strings.Builder
	chunkCount := 0
	resp, err := client.SendMessageStreaming(ctx, payload, func(chunk string) error {
		trimmed := strings.TrimSpace(chunk)
		// First callback in streaming path is often a session ID notification.
		if strings.HasPrefix(trimmed, "ses_") && !strings.Contains(trimmed, " ") && len(trimmed) < 80 {
			return nil
		}
		// Ignore internal control signals used between opencode client and adapters.
		if strings.HasPrefix(chunk, "\x00flush") ||
			strings.HasPrefix(chunk, "\x00thinking:") ||
			strings.HasPrefix(chunk, "\x00tool:") ||
			strings.HasPrefix(chunk, "\x00step:") ||
			strings.HasPrefix(chunk, "\x00todo:") {
			return nil
		}
		chunks.WriteString(chunk)
		if trimmed != "" {
			chunkCount++
		}
		return nil
	})
	if err != nil {
		return resp, chunkCount, err
	}

	if strings.TrimSpace(resp.Reply) == "" {
		resp.Reply = chunks.String()
	}

	if minStreamChunks > 0 && chunkCount < minStreamChunks {
		return resp, chunkCount, fmt.Errorf("invalid stream result: received %d chunks (< %d)", chunkCount, minStreamChunks)
	}

	return resp, chunkCount, nil
}

func buildTurnMessage(turn, repeatBase, repeatJitter int) string {
	// Repeat mixed Chinese/English text to grow context while preserving deterministic content.
	base := fmt.Sprintf("[turn:%d] 请复述这段话的主题并简短回答。The quick brown fox jumps over the lazy dog. ", turn)
	repeat := repeatBase
	if repeatJitter > 0 {
		repeat += (turn % repeatJitter)
	}
	if repeat < 1 {
		repeat = 1
	}
	return strings.Repeat(base, repeat)
}

func isGarbled(s string) (bool, string) {
	if s == "" {
		return false, ""
	}
	if !utf8.ValidString(s) {
		return true, "invalid-utf8"
	}
	if strings.ContainsRune(s, '�') {
		return true, "contains-replacement-char"
	}
	if strings.Contains(s, "\\uFFFD") {
		return true, "contains-escaped-replacement-char"
	}
	controlCount := 0
	for _, r := range s {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			controlCount++
		}
	}
	if hasMojibakeHint(s) {
		return true, "possible-mojibake-pattern"
	}
	if controlCount > 0 {
		return true, fmt.Sprintf("unexpected-control-chars:%d", controlCount)
	}
	return false, ""
}

func hasMojibakeHint(s string) bool {
	if s == "" {
		return false
	}
	markers := []string{"Ã", "Â", "ðŸ", "ä¸", "ï¿½", "鐢", "涓", "鍙"}
	for _, m := range markers {
		if strings.Contains(s, m) {
			return true
		}
	}

	nonASCII := 0
	for _, r := range s {
		if r > unicode.MaxASCII {
			nonASCII++
		}
	}
	if nonASCII == 0 {
		return false
	}

	brokenSeq := 0
	for i := 0; i < len(s)-2; i++ {
		if s[i] == '\xC3' || s[i] == '\xC2' {
			next, _ := utf8.DecodeRuneInString(s[i+1:])
			if next == utf8.RuneError {
				brokenSeq++
			}
		}
	}
	return brokenSeq >= 2
}

func hasGarbled(results []phaseResult) bool {
	for _, r := range results {
		if r.Garbled {
			return true
		}
	}
	return false
}

func hasErrors(results []phaseResult) bool {
	for _, r := range results {
		if r.Err != nil {
			return true
		}
	}
	return false
}

func printSummary(results []phaseResult) {
	fmt.Println()
	fmt.Println("Summary")
	fmt.Println("=======")
	if len(results) == 0 {
		fmt.Println("No turns executed")
		return
	}

	firstGarbledTurn := -1
	firstGarbledUsage := 0.0

	for _, r := range results {
		if r.Err != nil {
			fmt.Printf("turn=%02d phase=%.0f%% error=%v\n", r.Turn, r.PhaseTarget*100, r.Err)
			continue
		}
		fmt.Printf("turn=%02d phase=%.0f%% usage=%.1f%% tokens=%d/%d",
			r.Turn,
			r.PhaseTarget*100,
			r.Usage*100,
			r.TokenCount,
			r.ContextLen,
		)
		if r.StreamChunks > 0 {
			fmt.Printf(" stream_chunks=%d", r.StreamChunks)
		}
		fmt.Printf(" garbled=%v", r.Garbled)
		if r.Reason != "" {
			fmt.Printf(" reason=%s", r.Reason)
		}
		fmt.Println()

		if r.Garbled && firstGarbledTurn == -1 {
			firstGarbledTurn = r.Turn
			firstGarbledUsage = r.Usage
		}
	}

	if firstGarbledTurn == -1 {
		fmt.Println("Result: no garbled output detected in this run")
	} else {
		fmt.Printf("Result: first garbled output at turn %d (usage %.1f%%)\n", firstGarbledTurn, firstGarbledUsage*100)
	}
}

func estimateTokens(text string) int {
	if text == "" {
		return 0
	}

	runes := []rune(text)
	tokens := 0
	inWord := false

	for _, r := range runes {
		if r >= 0x4E00 && r <= 0x9FFF {
			tokens += 2
		} else if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			if !inWord {
				tokens++
				inWord = true
			}
		} else {
			inWord = false
			if r != ' ' && r != '\t' && r != '\n' {
				tokens++
			}
		}
	}

	return int(float64(tokens) * 1.3)
}
