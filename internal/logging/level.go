package logging

import (
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

// Level defines log verbosity threshold.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "info"
	}
}

// ParseLevel parses a log level string.
func ParseLevel(raw string) (Level, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return LevelDebug, true
	case "info", "":
		return LevelInfo, true
	case "warn", "warning":
		return LevelWarn, true
	case "error":
		return LevelError, true
	default:
		return LevelInfo, false
	}
}

// Install configures the standard logger with level filtering.
func Install(level Level) {
	log.SetOutput(&levelWriter{
		minLevel: level,
		dest:     os.Stderr,
	})
}

type levelWriter struct {
	minLevel Level
	dest     io.Writer
	mu       sync.Mutex
}

func (w *levelWriter) Write(p []byte) (int, error) {
	if w == nil {
		return len(p), nil
	}
	msgLevel := detectLevel(string(p))
	if msgLevel < w.minLevel {
		return len(p), nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.dest.Write(p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func detectLevel(msg string) Level {
	lower := strings.ToLower(msg)

	if strings.Contains(lower, "warn") || strings.Contains(lower, "warning") {
		return LevelWarn
	}

	if strings.Contains(lower, "error") || strings.Contains(lower, "fatal") || strings.Contains(lower, "failed") || strings.Contains(lower, "panic") {
		return LevelError
	}

	if strings.Contains(lower, "debug") {
		return LevelDebug
	}

	return LevelInfo
}
