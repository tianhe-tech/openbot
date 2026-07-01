package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"runtime"

	"github.com/fsnotify/fsnotify"
	opencodesdk "github.com/sst/opencode-sdk-go"
	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/adapters/dingtalk"
	"github.com/user/opencode-gateway/internal/adapters/feishu"
	"github.com/user/opencode-gateway/internal/adapters/wechat"
	"github.com/user/opencode-gateway/internal/adapters/wecom"
	"github.com/user/opencode-gateway/internal/asyncwork"
	"github.com/user/opencode-gateway/internal/config"
	"github.com/user/opencode-gateway/internal/logging"
	"github.com/user/opencode-gateway/internal/memstore"
	"github.com/user/opencode-gateway/internal/opencode"
	"github.com/user/opencode-gateway/internal/opencodesvc"
	"github.com/user/opencode-gateway/internal/proxy"
	"github.com/user/opencode-gateway/internal/retryworker"
	"github.com/user/opencode-gateway/internal/scheduler"
	"github.com/user/opencode-gateway/internal/server"
	"github.com/user/opencode-gateway/internal/skillgen"
	"github.com/user/opencode-gateway/internal/uibrpc"
)

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	logLevelFlag := flag.String("log-level", "", "log level: debug|info|warn|error")
	flag.Parse()

	fileLogCfg, err := config.LoadLogConfigFile(*configPath)
	if err != nil {
		log.Fatalf("load config file error: %v", err)
	}

	resolvedLogLevel, levelSource := config.ResolveLogLevel(*logLevelFlag, fileLogCfg)
	level, ok := logging.ParseLevel(resolvedLogLevel)
	if !ok {
		log.Fatalf("invalid log level %q, expected one of: debug|info|warn|error", resolvedLogLevel)
	}
	logging.Install(level)
	log.Printf("log level set to %s (source=%s)", level.String(), levelSource)

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	proxyKey := ""

	fmt.Println("cfg.ProxyHubWSURL:", cfg.ProxyHubWSURL)
	if cfg.ProxyHubWSURL != "" {
		proxyKey, err = proxy.PrepareRuntimeKey(cfg.ProxyKeyFile, cfg.ProxyHubWSURL)
		if err != nil {
			log.Fatalf("proxy key init error: %v", err)
		}
		log.Printf("proxy tunnel key ready (reused or generated), file=%s", cfg.ProxyKeyFile)

		go proxy.StartGatewayTunnel(ctx, cfg.ProxyHubWSURL, proxyKey, cfg.ProxyLocalAddr, cfg.ProxyReconnect)
	}

	// Create adapter registry for bidirectional communication
	adapterRegistry := base.NewAdapterRegistry()
	opencodeEndpoint := cfg.OpenCodeEndpoint
	serveArgs := append([]string(nil), cfg.OpenCodeServeArgs...)
	endpointSource := "config"
	autoSelectedEndpoint := false

	// Keep managed opencode serve port and client endpoint consistent.
	if cfg.OpenCodeManageServe {
		endpointFromEnv := strings.TrimSpace(os.Getenv("OPENCODE_ENDPOINT"))
		if endpointFromEnv == "" {
			// Endpoint not explicitly configured: avoid hard-binding to 4096 when occupied.
			if p, err := pickFreeTCPPort(); err == nil {
				opencodeEndpoint = fmt.Sprintf("http://127.0.0.1:%d", p)
				endpointSource = "auto"
				autoSelectedEndpoint = true
				log.Printf("opencode: OPENCODE_ENDPOINT not set, auto-selected managed endpoint %s", opencodeEndpoint)
			}
		} else {
			endpointSource = "env"
		}

		if isLocalEndpoint(opencodeEndpoint) && !serveArgsContainPort(serveArgs) {
			if p, ok := endpointPort(opencodeEndpoint); ok {
				serveArgs = append(serveArgs, "--port", strconv.Itoa(p))
				log.Printf("opencode: forcing managed serve port to %d to match endpoint %s", p, opencodeEndpoint)
			}
		}
	}

	var serveManager *opencodesvc.Manager
	if cfg.OpenCodeManageServe {
		serveManager = opencodesvc.NewManager(cfg.OpenCodeDirectory, cfg.OpenCodeServeCommand, serveArgs)
		msg, startErr := serveManager.Start(ctx)
		if startErr != nil {
			log.Fatalf("opencode serve manager start error: %v", startErr)
		}
		log.Printf("%s", msg)
		// Keep the process alive: restart automatically whenever it exits.
		go serveManager.RunWithAutoRestart(ctx, opencodeEndpoint)
		defer func() {
			if err := serveManager.Stop(); err != nil {
				log.Printf("stop opencode serve error: %v", err)
			}
		}()
	}

	// Create OpenCode client with event handling support
	ocOptions := []opencode.Option{
		opencode.WithDirectory(cfg.OpenCodeDirectory),
		opencode.WithDevCoreProfile(cfg.OpenCodeDevCoreEnabled, cfg.OpenCodeDevCorePrompt),
	}
	if serveManager != nil {
		ocOptions = append(ocOptions, opencode.WithServerUnavailableHandler(func(restartCtx context.Context, reason string) (string, error) {
			msg, err := serveManager.Start(restartCtx)
			if err != nil {
				return msg, err
			}
			log.Printf("opencode: waiting for server to become ready (%s)...", opencodeEndpoint)
			if waitErr := serveManager.WaitReady(restartCtx, opencodeEndpoint); waitErr != nil {
				log.Printf("opencode: server readiness wait ended: %v", waitErr)
			}
			return msg, nil
		}))
	}

	// ========== Memory Store (optional) ==========
	memStorePath := cfg.MemStorePath
	if memStorePath == "" {
		// Default: <OPENCODE_DIRECTORY>/tmp/memory.db
		memStorePath = filepath.Join(cfg.OpenCodeDirectory, "mem", "memory.db")
	}
	memDB, memErr := memstore.Open(memStorePath)
	if memErr != nil {
		log.Printf("main: memory store unavailable (%s), continuing without memory: %v", memStorePath, memErr)
	} else {
		log.Printf("main: memory store opened at %s", memStorePath)
		ocOptions = append(ocOptions, opencode.WithMemStore(memstore.NewGatewayAdapter(memDB)))
		// Auto-register the MCP memory tool so the LLM can search memory proactively.
		ensureMCPMemoryTool(cfg.OpenCodeDirectory, memStorePath)
		// Periodic forgetting curve decay (every hour)
		go func() {
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := memDB.DecayAll(); err != nil {
						log.Printf("memstore: decay error: %v", err)
					}
				}
			}
		}()
		defer func() {
			if err := memDB.Close(); err != nil {
				log.Printf("memstore: close error: %v", err)
			}
		}()
	}

	ocClient := opencode.NewClient(opencodeEndpoint, cfg.OpenCodeAPIKey, ocOptions...)

	// ========== Async work queue (handoff save, skill mining) ==========
	asyncQueue := asyncwork.New(cfg.SkillAutogen.QueueCapacity)
	asyncQueue.Start(ctx)
	ocClient.SetAsyncQueue(asyncQueue)
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		asyncQueue.Stop(stopCtx)
	}()

	// ========== Skill autogen (optional) ==========
	if cfg.SkillAutogen.Enabled && memDB != nil {
		skillCfg := skillgen.Config{
			Enabled:             true,
			DraftModel:          cfg.SkillAutogen.DraftModel,
			AlternateModels:     cfg.SkillAutogen.AlternateModels,
			Epsilon:             cfg.SkillAutogen.Epsilon,
			ModelSelfSelect:     cfg.SkillAutogen.ModelSelfSelect,
			MaxPerDay:           cfg.SkillAutogen.MaxPerDay,
			OnHandoff:           cfg.SkillAutogen.OnHandoff,
			OnLongSession:       cfg.SkillAutogen.OnLongSession,
			LongSessionMinTurns: cfg.SkillAutogen.LongSessionMinTurns,
			CandidateDir:        cfg.SkillAutogen.CandidateDir,
			InstallDir:          cfg.SkillAutogen.InstallDir,
			ApprovalRequired:    cfg.SkillAutogen.ApprovalRequired,
			MinConfidence:       cfg.SkillAutogen.MinConfidence,
			MinToolCalls:        cfg.SkillAutogen.MinToolCalls,
		}
		drafter := skillgen.NewOpencodeDrafter(ocClient, cfg.SkillAutogen.ReferenceSkillPath)
		notifier := skillgen.NewRegistryNotifier(adapterRegistry)
		skillSvc := skillgen.NewService(skillCfg, memDB, ocClient, asyncQueue, drafter, notifier)
		ocClient.SetSkillCandidateHook(skillSvc)
		ocClient.AddCommandInterceptor(skillSvc)
		log.Printf("main: skill autogen enabled (model=%s approvalRequired=%t maxPerDay=%d)",
			skillCfg.DraftModel, skillCfg.ApprovalRequired, skillCfg.MaxPerDay)
	} else if cfg.SkillAutogen.Enabled && memDB == nil {
		log.Printf("main: skill autogen disabled: memory store unavailable")
	}

	// Watch opencode.json for changes and trigger a graceful restart.
	// Started here (after ocClient is created) so the drain check can inspect
	// active streaming sessions before restarting the opencode process.
	if serveManager != nil {
		// Collect the config paths that are currently in effect:
		// global (~/.config/opencode/opencode.json) and/or project-local.
		var watchPaths []string
		globalCfgPath := filepath.Join(os.Getenv("HOME"), ".config", "opencode", "opencode.json")
		if runtime.GOOS == "windows" {
			if appData := os.Getenv("APPDATA"); appData != "" {
				globalCfgPath = filepath.Join(appData, "opencode", "opencode.json")
			}
		}
		projectCfgPath := filepath.Join(cfg.OpenCodeDirectory, "opencode.json")
		for _, p := range []string{globalCfgPath, projectCfgPath} {
			abs, _ := filepath.Abs(p)
			if _, err := os.Stat(abs); err == nil {
				watchPaths = append(watchPaths, abs)
			}
		}
		// If neither exists yet, still watch the project dir so creation is caught.
		if len(watchPaths) == 0 {
			abs, _ := filepath.Abs(projectCfgPath)
			watchPaths = append(watchPaths, abs)
		}
		restartOnChange := func() {
			log.Println("opencode: config file changed, draining active sessions before restart...")
			// Wait up to 30 s for any in-progress streaming to finish.
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				if !ocClient.HasActiveStreamingSessions() {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			log.Println("opencode: restarting opencode serve (config-change)...")
			if _, err := serveManager.Restart(ctx, "config-change"); err != nil {
				log.Printf("opencode: config-change restart error: %v", err)
			}
		}
		go watchOpencodeConfig(ctx, watchPaths, restartOnChange)
	}

	if cfg.ProxyHubWSURL != "" {
		restartFn := func(restartCtx context.Context, reason string) (string, error) {
			if serveManager == nil {
				return "opencode serve manager disabled (OPENCODE_MANAGE_SERVE=false)", nil
			}
			return serveManager.Restart(restartCtx, reason)
		}
		rpcHandler := uibrpc.NewServer(proxyKey, ocClient, restartFn)
		go proxy.StartGatewayRPCClient(ctx, cfg.ProxyHubWSURL, proxyKey, cfg.ProxyReconnect, rpcHandler.Handle)
	}

	// ========== Create Task Scheduler ==========
	schedulerCfg := scheduler.DefaultTaskSchedulerConfig()
	taskScheduler := scheduler.NewTaskScheduler(ocClient, schedulerCfg)
	cronScheduler := scheduler.NewCronScheduler(taskScheduler)

	// Set cron task storage path for persistence
	cronStoragePath := filepath.Join(cfg.OpenCodeDirectory, "cron", "cron_tasks.json")
	cronScheduler.SetStoragePath(cronStoragePath)
	log.Printf("main: cron tasks will be persisted to %s", cronStoragePath)

	webhookHandler := scheduler.NewWebhookHandler(taskScheduler, cronScheduler)

	// Create adapters
	wecomHandler := wecom.NewHandler(ocClient, cfg.WeCom)
	feishuHandler := feishu.NewHandler(ocClient, cfg.FeiShu)
	dingtalkHandler := dingtalk.NewHandler(ocClient, cfg.DingTalk)
	wechatHandler := wechat.NewHandler(ocClient, cfg.WeChat)

	// Set cronScheduler to adapters so they can manage scheduled tasks
	dingtalkHandler.SetCronScheduler(cronScheduler)
	feishuHandler.SetCronScheduler(cronScheduler)
	wecomHandler.SetCronScheduler(cronScheduler)
	wechatHandler.SetCronScheduler(cronScheduler)
	nlScheduleSvc := scheduler.NewNLScheduleService(cronScheduler, taskScheduler, nil, nil)
	dingtalkHandler.SetNLScheduleService(nlScheduleSvc)
	feishuHandler.SetNLScheduleService(nlScheduleSvc)
	wecomHandler.SetNLScheduleService(nlScheduleSvc)
	wechatHandler.SetNLScheduleService(nlScheduleSvc)

	// ========== Offline retry queue (optional) ==========
	if cfg.RetryQueue.Enabled && memDB != nil {
		rwCfg := retryworker.Config{
			Enabled:        true,
			CronExpr:       cfg.RetryQueue.CronExpr,
			MaxRetries:     cfg.RetryQueue.MaxRetries,
			BatchSize:      cfg.RetryQueue.BatchSize,
			MessageTimeout: 25 * time.Minute,
		}
		rwRegistry := retryworker.NewRegistry()
		rw := retryworker.New(rwCfg, memDB, rwRegistry)

		// Wire adapters as retry senders.
		rwRegistry.Register(feishuHandler)
		rwRegistry.Register(dingtalkHandler)

		// Give adapters access to the retry store + worker (for /retry command
		// and for enqueuing on timeout).
		feishuHandler.SetRetryWorker(memDB, rw)
		dingtalkHandler.SetRetryWorker(memDB, rw)

		// Schedule cron-driven off-peak processing.
		if cfg.RetryQueue.CronExpr != "" {
			if sysErr := cronScheduler.AddSystemJob(cfg.RetryQueue.CronExpr, func() {
				runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
				defer cancel()
				succ, total, err := rw.RunOnce(runCtx)
				if err != nil {
					log.Printf("retry-cron: error: %v", err)
					return
				}
				log.Printf("retry-cron: succeeded=%d total=%d", succ, total)
			}); sysErr != nil {
				log.Printf("main: retry queue cron registration error: %v", sysErr)
			} else {
				log.Printf("main: retry queue cron registered (expr=%q)", cfg.RetryQueue.CronExpr)
			}
		}
		// Periodic purge of expired records (every 6 hours).
		go func() {
			ticker := time.NewTicker(6 * time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := memDB.PurgeExpiredRetries(); err != nil {
						log.Printf("retry-queue: purge error: %v", err)
					}
				}
			}
		}()
		log.Printf("main: retry queue enabled (cron=%q maxRetries=%d batchSize=%d)",
			cfg.RetryQueue.CronExpr, cfg.RetryQueue.MaxRetries, cfg.RetryQueue.BatchSize)
	} else if cfg.RetryQueue.Enabled && memDB == nil {
		log.Printf("main: retry queue disabled: memory store unavailable")
	}

	// Register adapters in registry
	adapterRegistry.Register(wecomHandler.GetAdapter())
	adapterRegistry.Register(feishuHandler.GetAdapter())
	adapterRegistry.Register(dingtalkHandler.GetAdapter())
	adapterRegistry.Register(wechatHandler.GetAdapter())

	// erroredSessions tracks sessions that were deliberately cleared after session.error.
	// The title-recovery logic must not re-map these, otherwise the user gets stuck on
	// the broken session forever.
	var erroredSessions sync.Map // map[sessionID]struct{}

	// Register event handler for OpenCode -> Adapter communication
	ocClient.RegisterEventHandler(func(ctx context.Context, event *opencodesdk.EventListResponse) error {
		eventType := string(event.Type)
		log.Printf("🔍 MAIN EVENT HANDLER - type=%s", eventType)

		// Extract sessionID from event
		sessionID := extractSessionIDFromEvent(event)
		if sessionID == "" {
			log.Printf("opencode event: no sessionID found in event type %s, JSON preview: %.200s",
				eventType, event.JSON.RawJSON())
			return nil
		}

		// Try to find the user for this session across all adapters
		var foundAdapter *base.BidirectionalAdapter
		var foundUserID string
		var foundChannel string
		var isCronSession bool

		for _, adapter := range []struct {
			name    string
			handler interface {
				GetAdapter() *base.BidirectionalAdapter
			}
		}{
			{"dingtalk", dingtalkHandler},
			{"feishu", feishuHandler},
			{"wecom", wecomHandler},
			{"wechat", wechatHandler},
		} {
			adapter := adapter.handler.GetAdapter()
			if userID, ok := adapter.GetUserForSession(sessionID); ok {
				foundAdapter = adapter
				foundUserID = userID
				foundChannel = adapter.Name()
				isCronSession = strings.HasPrefix(userID, "cron:")
				log.Printf("opencode event: found session %s in adapter %s for user %s",
					sessionID[:min(8, len(sessionID))], foundChannel, foundUserID)
				break
			}
		}

		// 如果没有找到adapter，检查是否为定时任务session
		if foundAdapter == nil {
			if cronInfo, ok := taskScheduler.GetCronSessionInfo(sessionID); ok {
				// 这是一个定时任务session，尝试注册到adapter
				isCronSession = true
				log.Printf("opencode event: session %s belongs to cron task %s (adapter: %s)",
					sessionID[:min(8, len(sessionID))], cronInfo.TaskID, cronInfo.AdapterType)

				switch cronInfo.AdapterType {
				case "dingtalk":
					foundAdapter = dingtalkHandler.GetAdapter()
				case "feishu":
					foundAdapter = feishuHandler.GetAdapter()
				case "wecom":
					foundAdapter = wecomHandler.GetAdapter()
				case "wechat":
					foundAdapter = wechatHandler.GetAdapter()
				}

				if foundAdapter != nil {
					foundChannel = cronInfo.AdapterType
					foundUserID = fmt.Sprintf("cron:%s", sessionID[:min(12, len(sessionID))])
					// 注册session到adapter以减少后续查找
					foundAdapter.MapUserToSession(foundUserID, sessionID)
				}
			}
		}

		if foundAdapter == nil {
			// 跳过已被清除（session.error 后）的会话，避免 title 恢复逻辑把坏 session 重新绑定给用户
			if _, banned := erroredSessions.Load(sessionID); banned {
				return nil
			}

			// 尝试从 session Title 中恢复映射关系
			// Title 格式: [adapter:userId] threadId
			if session, err := ocClient.GetSession(ctx, sessionID); err == nil && session.Title != "" {
				title := session.Title
				// 解析 Title: [adapter:userId] threadId
				if len(title) > 0 && title[0] == '[' {
					if closeIdx := strings.Index(title, "]"); closeIdx > 0 {
						info := title[1:closeIdx] // 提取 "adapter:userId"
						parts := strings.SplitN(info, ":", 2)
						if len(parts) == 2 {
							adapterName := parts[0]
							userID := parts[1]

							// skillgen uses internal drafting sessions; they are not adapter-routable
							// and should not go through title recovery.
							if adapterName == "skillgen" {
								return nil
							}

							log.Printf("opencode event: recovering mapping from session title - adapter=%s, userID=%s, sessionID=%s",
								adapterName, userID, sessionID[:min(8, len(sessionID))])

							// 找到对应的 adapter 并恢复映射
							switch adapterName {
							case "dingtalk":
								foundAdapter = dingtalkHandler.GetAdapter()
							case "feishu":
								foundAdapter = feishuHandler.GetAdapter()
							case "wecom":
								foundAdapter = wecomHandler.GetAdapter()
							case "wechat":
								foundAdapter = wechatHandler.GetAdapter()
							}

							if foundAdapter != nil {
								foundChannel = adapterName
								foundUserID = userID
								// 恢复映射关系到内存
								foundAdapter.MapUserToSession(userID, sessionID)
								log.Printf("opencode event: successfully recovered mapping for session %s", sessionID[:min(8, len(sessionID))])
							}
						}
					}
				}
			}
		}

		if foundAdapter == nil {
			// 仅在非心跳和session状态事件时打日志，减少噪音
			if eventType != "server.heartbeat" && eventType != "session.status" &&
				eventType != "session.updated" && eventType != "session.diff" &&
				eventType != "message.updated" {
				log.Printf("opencode event: no adapter found for session %s (type=%s), skipping",
					sessionID[:min(8, len(sessionID))], eventType)
			}
			return nil
		}

		// 定时任务的权限请求自动审批
		if isCronSession && (eventType == "permission.asked" || eventType == "question.asked") {
			return handleCronPermission(ctx, ocClient, event, sessionID, eventType)
		}

		// Extract content from event based on type
		content, err := extractContentFromEvent(event)
		if err != nil {
			log.Printf("opencode event: failed to extract content from event type %s: %v", event.Type, err)
			return err
		}

		// 会话出错后清除 session 映射，使下一条消息自动建立新会话
		// 同时将该 sessionID 加入黑名单，防止 title 恢复逻辑把坏 session 重新绑定
		// 注意：必须在 content == "" 判断之前执行，否则清理逻辑会被跳过
		if eventType == "session.error" && !isCronSession {
			log.Printf("opencode event: clearing broken session %s for user %s (session.error)",
				sessionID[:min(8, len(sessionID))], foundUserID)
			foundAdapter.ClearSessionForUser(foundUserID)
			erroredSessions.Store(sessionID, struct{}{})
		}

		if content == "" {
			log.Printf("opencode event: no content extracted from event type %s for session %s",
				eventType, sessionID[:min(8, len(sessionID))])
			return nil
		}

		log.Printf("🔍 ROUTING TO ADAPTER - adapter=%s, session=%s, user=%s, content_len=%d, preview=%.100s",
			foundChannel, sessionID[:min(8, len(sessionID))], foundUserID, len(content), content)

		// Route to adapter
		routeErr := adapterRegistry.RouteEventToAdapter(ctx, foundChannel, sessionID, content)

		return routeErr
	})

	// Start event listener for bidirectional communication

	// Register adapters to taskScheduler for sending results
	taskScheduler.RegisterAdapter("dingtalk", dingtalkHandler)
	taskScheduler.RegisterAdapter("feishu", feishuHandler)
	taskScheduler.RegisterAdapter("wecom", wecomHandler)

	// Start task scheduler
	if err := taskScheduler.Start(); err != nil {
		log.Fatalf("failed to start task scheduler: %v", err)
	}
	defer taskScheduler.Stop()

	// Start cron scheduler
	if err := cronScheduler.Start(); err != nil {
		log.Fatalf("failed to start cron scheduler: %v", err)
	}
	defer cronScheduler.Stop()

	if err := ocClient.StartEventListener(ctx); err != nil {
		log.Printf("warning: could not start event listener: %v", err)
	} else {
		log.Println("opencode event listener started")
	}

	// Start DingTalk Stream client if enabled
	if err := dingtalkHandler.Start(ctx); err != nil {
		log.Printf("warning: could not start dingtalk stream client: %v", err)
	}
	defer dingtalkHandler.Stop()

	// Start Feishu WebSocket client if enabled
	if err := feishuHandler.Start(ctx); err != nil {
		log.Printf("warning: could not start feishu websocket client: %v", err)
	}
	defer feishuHandler.Stop()

	// Start WeChat long-poll client if enabled
	if err := wechatHandler.Start(ctx); err != nil {
		log.Printf("warning: could not start wechat poll client: %v", err)
	}
	defer wechatHandler.Stop()

	// Setup HTTP server
	srv := server.New(server.Config{
		Addr:          cfg.ServerAddr,
		ReadTimeout:   cfg.ReadTimeout,
		WriteTimeout:  cfg.WriteTimeout,
		ShutdownGrace: cfg.ShutdownGrace,
	})

	mux := srv.Mux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	// Add diagnostic endpoint to view session mappings
	mux.HandleFunc("/debug/sessions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		type SessionInfo struct {
			Adapter  string `json:"adapter"`
			UserID   string `json:"user_id"`
			Sessions []struct {
				SessionID string `json:"session_id"`
			} `json:"sessions"`
		}

		var sessionMappings []SessionInfo

		_ = sessionMappings // 用于未来扩展

		for _, adapterInfo := range []struct {
			name    string
			handler interface {
				GetAdapter() *base.BidirectionalAdapter
			}
		}{
			{"dingtalk", dingtalkHandler},
			{"feishu", feishuHandler},
			{"wecom", wecomHandler},
		} {
			// Note: We can't iterate sync.Map directly without exposing internal state
			// This is a simplified version showing adapter names
			info := SessionInfo{
				Adapter: adapterInfo.name,
				UserID:  "(internal state - check logs)",
				Sessions: []struct {
					SessionID string `json:"session_id"`
				}{},
			}
			sessionMappings = append(sessionMappings, info)
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "ok",
			"note":     "Session mappings are stored in memory. Check application logs for detailed session->user mappings.",
			"adapters": sessionMappings,
			"opencode": map[string]interface{}{
				"endpoint":               opencodeEndpoint,
				"endpoint_source":        endpointSource,
				"endpoint_auto_selected": autoSelectedEndpoint,
				"manage_serve":           cfg.OpenCodeManageServe,
				"serve_command":          cfg.OpenCodeServeCommand,
				"serve_args":             serveArgs,
			},
		})
	})

	mux.HandleFunc("/debug/opencode", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		port, hasPort := endpointPort(opencodeEndpoint)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":                 "ok",
			"endpoint":               opencodeEndpoint,
			"endpoint_source":        endpointSource,
			"endpoint_auto_selected": autoSelectedEndpoint,
			"endpoint_port":          port,
			"endpoint_port_known":    hasPort,
			"manage_serve":           cfg.OpenCodeManageServe,
			"serve_command":          cfg.OpenCodeServeCommand,
			"serve_args":             serveArgs,
		})
	})

	// Register scheduler webhook routes
	webhookHandler.RegisterRoutes(mux)
	log.Println("scheduler webhook routes registered")

	// Mount adapters (webhook endpoints)
	wecomHandler.Mount(mux)
	feishuHandler.Mount(mux)
	dingtalkHandler.Mount(mux) // Only mounts if not using Stream mode

	log.Printf("opencode gateway ready on %s (bidirectional mode)", cfg.ServerAddr)

	// Log registered adapters and their modes
	var adapters []string
	adapters = append(adapters, "wecom (webhook)")
	if cfg.FeiShu.UseWebSocket {
		adapters = append(adapters, "feishu (websocket)")
	} else {
		adapters = append(adapters, "feishu (webhook)")
	}
	if cfg.DingTalk.UseStream {
		adapters = append(adapters, "dingtalk (stream)")
	} else {
		adapters = append(adapters, "dingtalk (webhook)")
	}
	log.Printf("adapters registered: %v", adapters)
	log.Printf("event listener: active")

	if cfg.HTTPEnabled {
		log.Printf("HTTP server enabled: listening on %s", cfg.ServerAddr)
		if err := srv.Run(ctx); err != nil && err != context.Canceled {
			log.Fatalf("server error: %v", err)
		}
	} else {
		log.Printf("HTTP server disabled by config (HTTP_ENABLED=false): skip listen on %s", cfg.ServerAddr)
		<-ctx.Done()
	}

	log.Println("gateway stopped")
}

func serveArgsContainPort(args []string) bool {
	for i := 0; i < len(args); i++ {
		a := strings.TrimSpace(args[i])
		if a == "--port" || a == "-p" {
			return true
		}
		if strings.HasPrefix(a, "--port=") {
			return true
		}
	}
	return false
}

func endpointPort(endpoint string) (int, bool) {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || u == nil {
		return 0, false
	}
	p := u.Port()
	if p == "" {
		switch strings.ToLower(u.Scheme) {
		case "http":
			return 80, true
		case "https":
			return 443, true
		default:
			return 0, false
		}
	}
	n, convErr := strconv.Atoi(p)
	if convErr != nil || n <= 0 || n > 65535 {
		return 0, false
	}
	return n, true
}

func isLocalEndpoint(endpoint string) bool {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || u == nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	return host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func pickFreeTCPPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok || addr.Port <= 0 {
		return 0, fmt.Errorf("failed to allocate free tcp port")
	}
	return addr.Port, nil
}

// watchOpencodeConfig monitors one or more opencode.json config files and calls
// onChange after a 2-second quiet period following any Write/Create/Rename event.
// It watches the parent directories so editor atomic-write renames are also caught.
func watchOpencodeConfig(ctx context.Context, configPaths []string, onChange func()) {
	if len(configPaths) == 0 {
		return
	}

	// Build absolute path set and collect unique parent directories to watch.
	absSet := make(map[string]struct{}, len(configPaths))
	dirSet := make(map[string]struct{}, len(configPaths))
	for _, p := range configPaths {
		abs, _ := filepath.Abs(p)
		absSet[abs] = struct{}{}
		dirSet[filepath.Dir(abs)] = struct{}{}
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("opencode: config watcher unavailable: %v", err)
		return
	}
	defer watcher.Close()

	for dir := range dirSet {
		if err := watcher.Add(dir); err != nil {
			log.Printf("opencode: config watcher add %s: %v", dir, err)
		}
	}
	log.Printf("opencode: watching config files: %v", configPaths)

	const debounceDuration = 2 * time.Second
	var debounce *time.Timer

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			abs, _ := filepath.Abs(event.Name)
			if _, matched := absSet[abs]; !matched {
				continue
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
				log.Printf("opencode: config change detected: %s", abs)
				if debounce != nil {
					debounce.Stop()
				}
				debounce = time.AfterFunc(debounceDuration, onChange)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("opencode: config watcher error: %v", err)
		}
	}
}

// extractSessionIDFromEvent 从OpenCode事件中提取sessionID
func extractSessionIDFromEvent(event *opencodesdk.EventListResponse) string {
	if event == nil || event.JSON.RawJSON() == "" {
		return ""
	}

	jsonData := event.JSON.RawJSON()

	// 尝试提取多个位置的sessionID
	sessionID := ""

	// 1. 从 properties 中提取 sessionID（支持多个位置）
	var propsWrapper struct {
		Properties struct {
			SessionID string `json:"sessionID"`
			Message   struct {
				SessionID string `json:"sessionID"`
			} `json:"message"`
			Info struct {
				SessionID string `json:"sessionID"`
				ID        string `json:"id"`
			} `json:"info"`
			Part struct {
				SessionID string `json:"sessionID"`
			} `json:"part"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &propsWrapper); err == nil {
		if propsWrapper.Properties.SessionID != "" {
			sessionID = propsWrapper.Properties.SessionID
		} else if propsWrapper.Properties.Message.SessionID != "" {
			sessionID = propsWrapper.Properties.Message.SessionID
		} else if propsWrapper.Properties.Part.SessionID != "" {
			sessionID = propsWrapper.Properties.Part.SessionID
		} else if propsWrapper.Properties.Info.SessionID != "" {
			sessionID = propsWrapper.Properties.Info.SessionID
		} else if propsWrapper.Properties.Info.ID != "" && strings.HasPrefix(propsWrapper.Properties.Info.ID, "ses_") {
			// session.created/session.updated events store session ID in properties.info.id
			sessionID = propsWrapper.Properties.Info.ID
		}
	}

	// 2. 从根级别 sessionID 提取
	if sessionID == "" {
		var rootWrapper struct {
			SessionID string `json:"sessionID"`
		}
		if err := json.Unmarshal([]byte(jsonData), &rootWrapper); err == nil {
			sessionID = rootWrapper.SessionID
		}
	}

	if sessionID != "" {
		return sessionID
	}

	// 3. 特殊处理：从 data 提取
	var dataWrapper struct {
		Data struct {
			SessionID string `json:"sessionID"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(jsonData), &dataWrapper); err == nil {
		return dataWrapper.Data.SessionID
	}

	return ""
}

// extractContentFromEvent 从OpenCode事件中提取内容
func extractContentFromEvent(event *opencodesdk.EventListResponse) (string, error) {
	if event == nil || event.JSON.RawJSON() == "" {
		return "", nil
	}

	eventType := string(event.Type)

	// 根据不同的事件类型提取内容
	switch eventType {
	case "message.part.updated":
		// 💡 重要：message.part.updated 由 StreamingSessionHandler 处理并通过 callback 发送
		// 这里不处理，避免重复发送。只返回空字符串告诉调用者忽略此事件。
		return "", nil

	case "question.asked", "permission.asked":
		// 💡 重要：question.asked/permission.asked 由 StreamingSessionHandler 处理并通过 callback 发送
		// 这里不处理，避免重复发送给adapter。
		return "", nil

	case "session.error":
		// session.error 已由 StreamingSessionHandler 处理（包括 overflow 检测和用户通知），
		// 全局处理器只负责清理 session 映射（在调用方完成），不再重复发送错误消息。
		return "", nil

	case "session.status", "session.updated", "session.created", "session.diff",
		"file.edited", "file.watcher.updated", "lsp.updated", "server.heartbeat", "message.updated":
		return "", nil

	case "session.idle":
		// 不发送额外提示，StreamingSessionHandler 已处理完成逻辑
		// 实际内容由流式回调发送，避免重复消息
		return "", nil
	}

	// 默认返回空字符串
	return "", nil
}

// extractQuestionFromEventJSON 从事件JSON中提取问题内容
func extractQuestionFromEventJSON(jsonData string) (*opencode.Question, string) {
	var wrapper struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		return nil, ""
	}

	if wrapper.Type == "permission.asked" {
		return extractPermissionQuestionJSON(jsonData)
	} else if wrapper.Type == "question.asked" {
		return extractNormalQuestionJSON(jsonData)
	}

	return nil, ""
}

// extractPermissionQuestionJSON 提取权限问题
func extractPermissionQuestionJSON(jsonData string) (*opencode.Question, string) {
	type PermissionProps struct {
		ID         string   `json:"id"`
		SessionID  string   `json:"sessionID"`
		Permission string   `json:"permission"`
		Patterns   []string `json:"patterns"`
		Metadata   struct {
			Filepath  string `json:"filepath"`
			ParentDir string `json:"parentDir"`
		} `json:"metadata"`
	}

	var wrapper struct {
		Properties PermissionProps `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		return nil, ""
	}

	props := wrapper.Properties
	if props.ID == "" {
		return nil, ""
	}

	var permDesc string
	switch props.Permission {
	case "external_directory":
		permDesc = "访问外部目录"
	case "write_file":
		permDesc = "写入文件"
	case "execute_command":
		permDesc = "执行命令"
	case "network_access":
		permDesc = "网络访问"
	default:
		permDesc = props.Permission
	}

	var details string
	if len(props.Patterns) > 0 {
		details = "路径: " + strings.Join(props.Patterns, ", ")
	}
	if props.Metadata.Filepath != "" {
		if details != "" {
			details += "\\n"
		}
		details += "文件: " + props.Metadata.Filepath
	}

	msg := fmt.Sprintf("🔐 OpenCode 请求权限：\\n\\n"+
		"【%s】\\n\\n%s\\n\\n"+
		"请回复：\\n"+
		"• 允许 - 本次允许\\n"+
		"• 拒绝 - 拒绝此请求\\n"+
		"• 始终允许 - 以后都允许",
		permDesc, details)

	return &opencode.Question{
		ID:           props.ID,
		SessionID:    props.SessionID,
		Text:         fmt.Sprintf("%s\\n%s", permDesc, details),
		Options:      []string{"允许", "拒绝", "始终允许"},
		IsPermission: true,
		Directory:    props.Metadata.ParentDir,
	}, msg
}

// extractNormalQuestionJSON 提取普通问题
func extractNormalQuestionJSON(jsonData string) (*opencode.Question, string) {
	type jsonOptionItem struct {
		Label       string `json:"label"`
		Description string `json:"description"`
	}

	type jsonQuestionItem struct {
		Header   string           `json:"header"`
		Question string           `json:"question"`
		Multiple bool             `json:"multiple"`
		Options  []jsonOptionItem `json:"options"`
	}

	type QuestionProps struct {
		ID        string             `json:"id"`
		SessionID string             `json:"sessionID"`
		Questions []jsonQuestionItem `json:"questions"`
		Question  string             `json:"question"`
		Text      string             `json:"text"`
		Options   []string           `json:"options"`
	}

	var wrapper struct {
		Properties QuestionProps `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		return nil, ""
	}

	props := wrapper.Properties
	questionText := props.Question
	if questionText == "" {
		questionText = props.Text
	}

	if questionText == "" {
		return nil, ""
	}

	var msgBuilder strings.Builder
	msgBuilder.WriteString("❓ OpenCode 需要您的选择：\\n\\n")
	msgBuilder.WriteString(questionText)

	if len(props.Options) > 0 {
		msgBuilder.WriteString("\\n请选择：\\n")
		for i, opt := range props.Options {
			msgBuilder.WriteString(fmt.Sprintf("%d. %s\\n", i+1, opt))
		}
		msgBuilder.WriteString("\\n直接回复选项序号（如 `1`）或选项名称")
	} else {
		msgBuilder.WriteString("\\n回复 `yes` 确认，或 `no` 取消")
	}

	return &opencode.Question{
		ID:        props.ID,
		SessionID: props.SessionID,
		Text:      questionText,
		Options:   props.Options,
	}, msgBuilder.String()
}

// handleCronPermission 处理定时任务session的权限请求（自动审批）
func handleCronPermission(ctx context.Context, ocClient *opencode.Client, event *opencodesdk.EventListResponse, sessionID, eventType string) error {
	jsonData := event.JSON.RawJSON()
	if jsonData == "" {
		return nil
	}

	log.Printf("opencode cron: auto-handling %s for cron session %s", eventType, sessionID[:min(8, len(sessionID))])

	// 提取问题/权限信息
	question, _ := extractQuestionFromEventJSON(jsonData)
	if question == nil {
		log.Printf("opencode cron: could not extract question from %s event for session %s", eventType, sessionID[:min(8, len(sessionID))])
		return nil
	}

	// 存储问题以便 AnswerQuestion 能找到
	ocClient.StorePendingQuestion(question)

	// 自动审批：权限请求选择"允许"，普通问题选择第一个选项
	var answer string
	if question.IsPermission {
		answer = "允许"
		log.Printf("opencode cron: auto-approving permission %s for cron session %s", question.ID, sessionID[:min(8, len(sessionID))])
	} else if len(question.Options) > 0 {
		answer = question.Options[0]
		log.Printf("opencode cron: auto-selecting first option '%s' for question %s in cron session %s",
			answer, question.ID, sessionID[:min(8, len(sessionID))])
	} else {
		answer = "yes"
		log.Printf("opencode cron: auto-answering 'yes' for question %s in cron session %s",
			question.ID, sessionID[:min(8, len(sessionID))])
	}

	if err := ocClient.AnswerQuestion(ctx, question.ID, answer); err != nil {
		log.Printf("opencode cron: failed to auto-answer %s for session %s: %v",
			eventType, sessionID[:min(8, len(sessionID))], err)
		return err
	}

	log.Printf("opencode cron: successfully auto-answered %s for cron session %s", eventType, sessionID[:min(8, len(sessionID))])
	return nil
}

// ========== MCP memory tool auto-registration ==========

// ensureMCPMemoryTool ensures the project-level opencode.json contains a
// "gateway-memory" MCP entry so the LLM can call memory tools without manual
// configuration.  If the entry already exists it is left untouched.
func ensureMCPMemoryTool(opencodeDir, memDBPath string) {
	mcpBin := findMCPBinary()
	if mcpBin == "" {
		log.Printf("mcp: openbot-mcp binary not found next to gateway, skipping auto-registration")
		return
	}

	// Resolve memDBPath to absolute so the MCP process can find it regardless of cwd.
	if abs, err := filepath.Abs(memDBPath); err == nil {
		memDBPath = abs
	}

	cfgPath := filepath.Join(opencodeDir, "opencode.json")

	// Read existing config or start fresh.
	var root map[string]interface{}
	data, err := os.ReadFile(cfgPath)
	if err == nil {
		if err := json.Unmarshal(data, &root); err != nil {
			log.Printf("mcp: cannot parse %s, skipping auto-registration: %v", cfgPath, err)
			return
		}
	} else {
		root = make(map[string]interface{})
	}

	// Navigate into "mcp" section.
	mcpSection, _ := root["mcp"].(map[string]interface{})
	if mcpSection == nil {
		mcpSection = make(map[string]interface{})
	}
	if _, exists := mcpSection["gateway-memory"]; exists {
		return // already registered
	}

	mcpSection["gateway-memory"] = map[string]interface{}{
		"type":    "local",
		"command": []string{mcpBin, "--db", memDBPath},
		"enabled": true,
	}
	root["mcp"] = mcpSection

	out, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		log.Printf("mcp: marshal error: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
		log.Printf("mcp: mkdir error: %v", err)
		return
	}
	if err := os.WriteFile(cfgPath, out, 0o644); err != nil {
		log.Printf("mcp: write %s error: %v", cfgPath, err)
		return
	}
	log.Printf("mcp: auto-registered gateway-memory tool in %s (binary=%s)", cfgPath, mcpBin)
}

// findMCPBinary locates the openbot-mcp binary next to the current executable,
// in the working directory, or in ./bin/.
func findMCPBinary() string {
	suffix := ""
	if runtime.GOOS == "windows" {
		suffix = ".exe"
	}
	name := "openbot-mcp" + suffix

	// 1. Next to the current executable.
	if exe, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(exe), name)
		if _, err := os.Stat(candidate); err == nil {
			abs, _ := filepath.Abs(candidate)
			return abs
		}
	}
	// 2. Current working directory.
	if abs, err := filepath.Abs(name); err == nil {
		if _, err := os.Stat(abs); err == nil {
			return abs
		}
	}
	// 3. ./bin/ directory.
	if abs, err := filepath.Abs(filepath.Join("bin", name)); err == nil {
		if _, err := os.Stat(abs); err == nil {
			return abs
		}
	}
	return ""
}
