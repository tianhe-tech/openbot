package opencodesvc

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Manager keeps a single opencode serve process under gateway control.
type Manager struct {
	mu      sync.Mutex
	workDir string
	command string
	args    []string
	cmd     *exec.Cmd
}

func NewManager(workDir, command string, args []string) *Manager {
	wd := strings.TrimSpace(workDir)
	if wd == "" {
		wd = "."
	}
	if abs, err := filepath.Abs(wd); err == nil {
		wd = abs
	}
	bin := strings.TrimSpace(command)
	if bin == "" {
		bin = "opencode"
	}
	resolvedArgs := make([]string, 0, len(args))
	for _, a := range args {
		v := strings.TrimSpace(a)
		if v != "" {
			resolvedArgs = append(resolvedArgs, v)
		}
	}
	if len(resolvedArgs) == 0 {
		resolvedArgs = []string{"serve"}
	}

	return &Manager{workDir: wd, command: bin, args: resolvedArgs}
}

func (m *Manager) Start(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isRunningLocked() {
		return fmt.Sprintf("opencode serve already running (pid=%d)", m.cmd.Process.Pid), nil
	}

	cmd := exec.CommandContext(ctx, m.command, m.args...)
	cmd.Dir = m.workDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("start opencode serve failed: %w", err)
	}
	m.cmd = cmd
	go m.waitAndClear(cmd)

	return fmt.Sprintf("opencode serve started (pid=%d, dir=%s)", cmd.Process.Pid, m.workDir), nil
}

func (m *Manager) Restart(ctx context.Context, reason string) (string, error) {
	if err := m.Stop(); err != nil {
		return "", err
	}
	msg, err := m.Start(ctx)
	if err != nil {
		return "", err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "manual"
	}
	return "opencode serve restarted (reason=" + reason + ")\n" + msg, nil
}

func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopLocked()
}

func (m *Manager) stopLocked() error {
	if !m.isRunningLocked() {
		m.cmd = nil
		return nil
	}
	proc := m.cmd.Process
	if proc == nil {
		m.cmd = nil
		return nil
	}
	if err := proc.Kill(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "process already finished") {
		return fmt.Errorf("stop opencode serve failed: %w", err)
	}
	m.cmd = nil
	return nil
}

func (m *Manager) isRunningLocked() bool {
	if m.cmd == nil || m.cmd.Process == nil {
		return false
	}
	// ProcessState is set by cmd.Wait() once the process has exited.
	// If it's non-nil the process is already dead; clear the stale reference.
	if m.cmd.ProcessState != nil {
		m.cmd = nil
		return false
	}
	return true
}

func (m *Manager) waitAndClear(cmd *exec.Cmd) {
	_ = cmd.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cmd == cmd {
		m.cmd = nil
	}
}

// waitCurrentProc blocks until the process currently tracked by the manager exits
// or ctx is cancelled.  It polls the running flag rather than calling Wait() a
// second time, so it is safe to call concurrently with waitAndClear.
func (m *Manager) waitCurrentProc(ctx context.Context) {
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			m.mu.Lock()
			alive := m.isRunningLocked()
			m.mu.Unlock()
			if !alive {
				return
			}
		}
	}
}

// RunWithAutoRestart keeps the managed process alive until ctx is cancelled.
// On every unexpected exit it waits restartDelay then starts a fresh process.
// If endpoint is non-empty it additionally waits up to 30 s for the HTTP
// endpoint to become reachable before considering the start successful.
// Call this in a goroutine after the initial Start().
func (m *Manager) RunWithAutoRestart(ctx context.Context, endpoint string) {
	const restartDelay = time.Second
	const readyTimeout = 30 * time.Second

	for {
		// Start (or confirm already running).
		msg, err := m.Start(ctx)
		if err != nil {
			log.Printf("opencodesvc: auto-restart start error: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(restartDelay):
			}
			continue
		}
		if !strings.Contains(msg, "already running") {
			log.Printf("opencodesvc: auto-restart: %s", msg)
		}

		// Wait for the HTTP endpoint to become reachable.
		if endpoint != "" {
			waitCtx, cancel := context.WithTimeout(ctx, readyTimeout)
			wErr := m.WaitReady(waitCtx, endpoint)
			cancel()
			if wErr != nil {
				select {
				case <-ctx.Done():
					return
				default:
				}
				log.Printf("opencodesvc: auto-restart wait-ready failed: %v", wErr)
				continue
			}
		}

		// Block here until the process exits.
		m.waitCurrentProc(ctx)

		select {
		case <-ctx.Done():
			return
		default:
			log.Printf("opencodesvc: process exited unexpectedly, restarting in %s...", restartDelay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(restartDelay):
			}
		}
	}
}

// WaitReady polls endpoint with HTTP GET until it accepts connections or ctx expires.
// Any HTTP response (even 4xx/5xx) is treated as "ready"; only connection errors are retried.
func (m *Manager) WaitReady(ctx context.Context, endpoint string) error {
	pollClient := &http.Client{Timeout: 2 * time.Second}
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		return fmt.Errorf("empty endpoint")
	}
	target := endpoint + "/"
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return err
		}
		resp, err := pollClient.Do(req)
		if err == nil {
			resp.Body.Close()
			return nil // server is reachable
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("opencode did not become ready: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}
