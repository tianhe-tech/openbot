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

	// Create adapters
	wecomHandler := wecom.NewHandler(ocClient, cfg.WeCom)
	feishuHandler := feishu.NewHandler(ocClient, cfg.FeiShu)
	dingtalkHandler := dingtalk.NewHandler(ocClient, cfg.DingTalk)

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

	// Mount adapters (webhook endpoints)
	wecomHandler.Mount(mux)
	feishuHandler.Mount(mux)
	dingtalkHandler.Mount(mux) // Only mounts if not using Stream mode

	log.Printf("opencode gateway ready on %s (bidirectional mode)", cfg.ServerAddr)

	// Log registered adapters and their modes
	adapters := []string{"wecom", "feishu"}
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
