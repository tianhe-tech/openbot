//go:build aliyun_audio_check

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	nls "github.com/aliyun/alibabacloud-nls-go-sdk"
)

func main() {
	var (
		source     = flag.String("source", "dingtalk", "audio source label: dingtalk|feishu")
		filePath   = flag.String("file", "", "local audio file path")
		format     = flag.String("format", "auto", "audio format: auto|amr|amr-wb|pcm|wav|opu|opus")
		sampleRate = flag.Int("sample-rate", 0, "sample rate, 0 means auto by format")
		timeoutSec = flag.Int("timeout", 45, "ASR timeout seconds")
	)
	flag.Parse()

	if strings.TrimSpace(*filePath) == "" {
		log.Fatal("missing required -file")
	}

	akID := strings.TrimSpace(os.Getenv("ALIYUN_NLS_AKID"))
	akKey := strings.TrimSpace(os.Getenv("ALIYUN_NLS_AKKEY"))
	appKey := strings.TrimSpace(os.Getenv("ALIYUN_NLS_APPKEY"))
	if akID == "" || akKey == "" || appKey == "" {
		log.Fatal("missing ALIYUN_NLS_AKID / ALIYUN_NLS_AKKEY / ALIYUN_NLS_APPKEY")
	}

	audioBytes, err := os.ReadFile(*filePath)
	if err != nil {
		log.Fatalf("read file failed: %v", err)
	}

	resolvedFormat, resolvedRate, normalizedBytes := normalizeAudioForNLS(audioBytes, *format, *sampleRate)
	log.Printf("source=%s file=%s bytes=%d format=%s sampleRate=%d", *source, *filePath, len(normalizedBytes), resolvedFormat, resolvedRate)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
	defer cancel()

	text, err := transcribeByAliyunNLS(ctx, normalizedBytes, resolvedFormat, resolvedRate, appKey, akID, akKey)
	if err != nil {
		log.Fatalf("asr failed: %v", err)
	}

	fmt.Println("---------------- ASR Result ----------------")
	if strings.TrimSpace(text) == "" {
		fmt.Println("(empty text)")
	} else {
		fmt.Println(text)
	}
	fmt.Println("--------------------------------------------")
}

func normalizeAudioForNLS(data []byte, format string, sampleRate int) (string, int, []byte) {
	fmtLower := strings.ToLower(strings.TrimSpace(format))
	if fmtLower == "" {
		fmtLower = "auto"
	}

	if fmtLower == "auto" {
		switch {
		case len(data) >= 4 && string(data[:4]) == "OggS":
			return "opu", 16000, data
		case strings.HasPrefix(string(data), "#!AMR-WB\n"):
			return "amr-wb", 16000, data[len("#!AMR-WB\n"):]
		case strings.HasPrefix(string(data), "#!AMR\n"):
			return "amr", 8000, data[len("#!AMR\n"):]
		default:
			if sampleRate == 0 {
				sampleRate = 16000
			}
			return "pcm", sampleRate, data
		}
	}

	if fmtLower == "opus" {
		fmtLower = "opu"
	}
	if sampleRate == 0 {
		switch fmtLower {
		case "amr":
			sampleRate = 8000
		case "amr-wb", "opu", "wav", "pcm":
			sampleRate = 16000
		default:
			sampleRate = 16000
		}
	}

	if fmtLower == "amr" && strings.HasPrefix(string(data), "#!AMR\n") {
		data = data[len("#!AMR\n"):]
	}
	if fmtLower == "amr-wb" && strings.HasPrefix(string(data), "#!AMR-WB\n") {
		data = data[len("#!AMR-WB\n"):]
	}

	return fmtLower, sampleRate, data
}

func transcribeByAliyunNLS(ctx context.Context, audioBytes []byte, format string, sampleRate int, appKey, akID, akKey string) (string, error) {
	config, err := nls.NewConnectionConfigWithAKInfoDefault(
		nls.DEFAULT_URL,
		appKey,
		akID,
		akKey,
	)
	if err != nil {
		return "", fmt.Errorf("NLS connection config error: %w", err)
	}

	type callbackState struct {
		resultCh chan string
		errCh    chan error
		latest   string
		mu       sync.Mutex
	}
	state := &callbackState{
		resultCh: make(chan string, 1),
		errCh:    make(chan error, 1),
	}

	nlsLogger := nls.NewNlsLogger(io.Discard, "nls", log.LstdFlags)
	nlsLogger.SetLogSil(true)

	sr, err := nls.NewSpeechRecognition(config, nlsLogger,
		func(text string, p interface{}) { // taskFailed
			s := p.(*callbackState)
			select {
			case s.errCh <- fmt.Errorf("NLS task failed: %s", text):
			default:
			}
		},
		nil,
		func(text string, p interface{}) { // resultChanged
			s := p.(*callbackState)
			if recognized := extractNLSRecognizedText(text); recognized != "" {
				s.mu.Lock()
				s.latest = recognized
				s.mu.Unlock()
			}
		},
		func(text string, p interface{}) { // completed
			s := p.(*callbackState)
			log.Printf("NLS completed raw JSON: %.800s", text)
			recognized := extractNLSRecognizedText(text)
			if recognized == "" {
				s.mu.Lock()
				recognized = s.latest
				s.mu.Unlock()
			}
			log.Printf("NLS recognized text: %q", recognized)
			select {
			case s.resultCh <- recognized:
			default:
			}
		},
		func(p interface{}) {},
		state,
	)
	if err != nil {
		return "", fmt.Errorf("create NLS SpeechRecognition failed: %w", err)
	}
	defer sr.Shutdown()

	srParam := nls.DefaultSpeechRecognitionParam()
	srParam.Format = format
	srParam.SampleRate = sampleRate

	ready, err := sr.Start(srParam, nil)
	if err != nil {
		return "", fmt.Errorf("NLS SR Start error: %w", err)
	}
	select {
	case ok := <-ready:
		if !ok {
			return "", fmt.Errorf("NLS SR Start failed")
		}
	case <-time.After(15 * time.Second):
		return "", fmt.Errorf("NLS SR Start timeout")
	case <-ctx.Done():
		return "", ctx.Err()
	}

	const chunkSize = 3200
	for i := 0; i < len(audioBytes); i += chunkSize {
		select {
		case ferr := <-state.errCh:
			return "", ferr
		default:
		}
		end := i + chunkSize
		if end > len(audioBytes) {
			end = len(audioBytes)
		}
		if err := sr.SendAudioData(audioBytes[i:end]); err != nil {
			return "", fmt.Errorf("NLS SendAudioData error: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, err = sr.Stop(); err != nil {
		return "", fmt.Errorf("NLS SR Stop error: %w", err)
	}

	select {
	case text := <-state.resultCh:
		return text, nil
	case ferr := <-state.errCh:
		return "", ferr
	case <-time.After(30 * time.Second):
		return "", fmt.Errorf("NLS result timeout")
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func extractNLSRecognizedText(raw string) string {
	text := strings.TrimSpace(raw)
	if text == "" {
		return ""
	}

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(text), &obj); err != nil {
		return ""
	}

	lookup := func(m map[string]interface{}, keys ...string) string {
		cur := interface{}(m)
		for _, k := range keys {
			next, ok := cur.(map[string]interface{})
			if !ok {
				return ""
			}
			cur, ok = next[k]
			if !ok {
				return ""
			}
		}
		s, _ := cur.(string)
		return strings.TrimSpace(s)
	}

	candidates := []string{
		lookup(obj, "payload", "result"),
		lookup(obj, "payload", "text"),
		lookup(obj, "result"),
		lookup(obj, "text"),
		lookup(obj, "payload", "output", "text"),
		lookup(obj, "payload", "output", "sentence"),
	}

	for _, v := range candidates {
		if v != "" {
			return v
		}
	}

	return ""
}
