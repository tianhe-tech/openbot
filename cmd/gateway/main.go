package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	opencodesdk "github.com/sst/opencode-sdk-go"
	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/adapters/dingtalk"
	"github.com/user/opencode-gateway/internal/adapters/feishu"
	"github.com/user/opencode-gateway/internal/adapters/wecom"
	"github.com/user/opencode-gateway/internal/config"
	"github.com/user/opencode-gateway/internal/opencode"
	"github.com/user/opencode-gateway/internal/scheduler"
	"github.com/user/opencode-gateway/internal/server"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// Create adapter registry for bidirectional communication
	adapterRegistry := base.NewAdapterRegistry()

	// Create OpenCode client with event handling support
	ocClient := opencode.NewClient(cfg.OpenCodeEndpoint, cfg.OpenCodeAPIKey,
		opencode.WithDirectory("."),
	)

	// ========== Create Task Scheduler ==========
	schedulerCfg := scheduler.DefaultTaskSchedulerConfig()
	taskScheduler := scheduler.NewTaskScheduler(ocClient, schedulerCfg)
	cronScheduler := scheduler.NewCronScheduler(taskScheduler)
	webhookHandler := scheduler.NewWebhookHandler(taskScheduler, cronScheduler)

	// Create adapters
	wecomHandler := wecom.NewHandler(ocClient, cfg.WeCom)
	feishuHandler := feishu.NewHandler(ocClient, cfg.FeiShu)
	dingtalkHandler := dingtalk.NewHandler(ocClient, cfg.DingTalk)

	// Set cronScheduler to adapters so they can manage scheduled tasks
	dingtalkHandler.SetCronScheduler(cronScheduler)
	feishuHandler.SetCronScheduler(cronScheduler)
	// TODO: Add SetCronScheduler to other adapters if needed
	// wecomHandler.SetCronScheduler(cronScheduler)

	// Register adapters in registry
	adapterRegistry.Register(wecomHandler.GetAdapter())
	adapterRegistry.Register(feishuHandler.GetAdapter())
	adapterRegistry.Register(dingtalkHandler.GetAdapter())

	// Register event handler for OpenCode -> Adapter communication
	ocClient.RegisterEventHandler(func(ctx context.Context, event *opencodesdk.EventListResponse) error {
		log.Printf("received event from OpenCode server: type=%s", event.Type)

		// Extract event information and route to appropriate adapter
		// TODO: Parse event structure to extract channel, sessionID, and content
		// Example:
		// return adapterRegistry.RouteEventToAdapter(ctx, channel, sessionID, content)

		return nil
	})

	// Start event listener for bidirectional communication
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	if err := srv.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("server error: %v", err)
	}

	log.Println("gateway stopped")
}
