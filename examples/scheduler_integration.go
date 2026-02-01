package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/user/opencode-gateway/internal/adapters/dingtalk"
	"github.com/user/opencode-gateway/internal/opencode"
	"github.com/user/opencode-gateway/internal/scheduler"
	"github.com/user/opencode-gateway/internal/server"
)

// 这是一个完整的集成示例，展示如何使用任务调度系统

func main() {
	// 加载配置
	cfg := loadConfig()

	// 创建OpenCode客户端
	opencodeClient := opencode.NewClient(
		cfg.OpenCode.Endpoint,
		cfg.OpenCode.APIKey,
		opencode.WithDirectory(cfg.OpenCode.WorkDir),
		opencode.WithTimeout(30*time.Minute),
	)

	// ========== 创建任务调度系统 ==========

	// 1. 创建任务调度器
	schedulerCfg := scheduler.DefaultTaskSchedulerConfig()
	schedulerCfg.MaxConcurrentTasks = 10 // 最多10个并发任务
	schedulerCfg.MaxQueueSize = 1000     // 队列最大1000个任务
	schedulerCfg.TaskTimeout = 30 * time.Minute

	taskScheduler := scheduler.NewTaskScheduler(opencodeClient, schedulerCfg)

	// 2. 创建定时任务调度器
	cronScheduler := scheduler.NewCronScheduler(taskScheduler)

	// 3. 创建Webhook处理器
	webhookHandler := scheduler.NewWebhookHandler(taskScheduler, cronScheduler)

	// ========== 创建HTTP服务器 ==========

	serverCfg := server.Config{
		Addr:          ":8080",
		ReadTimeout:   30 * time.Second,
		WriteTimeout:  30 * time.Second,
		ShutdownGrace: 10 * time.Second,
	}

	httpServer := server.New(serverCfg)

	// 注册webhook路由
	webhookHandler.RegisterRoutes(httpServer.Mux())

	// ========== 创建Adapter ==========

	// DingTalk adapter
	dingtalkCfg := dingtalk.Config{
		ClientID:     cfg.DingTalk.ClientID,
		ClientSecret: cfg.DingTalk.ClientSecret,
		UseStream:    true,
	}

	dingtalkHandler := dingtalk.NewHandler(opencodeClient, dingtalkCfg)

	// 注册adapter到调度器
	// 注意：这里需要创建一个适配器包装器来实现SendMessage接口
	dingtalkSender := &DingTalkAdapterSender{handler: dingtalkHandler}
	taskScheduler.RegisterAdapter("dingtalk", dingtalkSender)

	// ========== 启动所有服务 ==========

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动任务调度器
	if err := taskScheduler.Start(); err != nil {
		log.Fatalf("Failed to start task scheduler: %v", err)
	}

	// 启动定时任务调度器
	if err := cronScheduler.Start(); err != nil {
		log.Fatalf("Failed to start cron scheduler: %v", err)
	}

	// 启动DingTalk adapter
	if err := dingtalkHandler.Start(ctx); err != nil {
		log.Fatalf("Failed to start dingtalk handler: %v", err)
	}

	// 添加一些示例定时任务
	setupExampleScheduledTasks(cronScheduler)

	// 启动HTTP服务器
	go func() {
		log.Printf("HTTP server listening on %s", serverCfg.Addr)
		if err := httpServer.Run(ctx); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// ========== 等待退出信号 ==========

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("Shutting down...")

	// 优雅关闭
	cancel()

	if err := cronScheduler.Stop(); err != nil {
		log.Printf("Error stopping cron scheduler: %v", err)
	}

	if err := taskScheduler.Stop(); err != nil {
		log.Printf("Error stopping task scheduler: %v", err)
	}

	dingtalkHandler.Stop()

	log.Println("Shutdown complete")
}

// DingTalkAdapterSender 实现AdapterSender接口
type DingTalkAdapterSender struct {
	handler *dingtalk.Handler
}

func (s *DingTalkAdapterSender) SendMessage(ctx context.Context, channel, userID, content string) error {
	// 这里需要实现发送消息到钉钉的逻辑
	// 具体实现取决于你的adapter设计
	log.Printf("Sending message to DingTalk: channel=%s, user=%s, content=%s", channel, userID, content)
	return nil
}

// setupExampleScheduledTasks 设置示例定时任务
func setupExampleScheduledTasks(cronScheduler *scheduler.CronScheduler) {
	// 示例1: 每小时执行一次监控任务
	monitoringTask := &scheduler.ScheduledTask{
		Name:        "社交媒体监控",
		Description: "每小时检查Twitter上的AI话题",
		Type:        scheduler.TaskTypeMonitoring,
		CronExpr:    "0 0 * * * *", // 每小时整点
		Enabled:     true,
		AdapterType: "dingtalk",
		Channel:     "ai_news",
		Content:     "搜索Twitter #AI话题，总结最新内容",
		Agent:       "social_monitor",
		Metadata: map[string]interface{}{
			"platform": "twitter",
			"keyword":  "#AI",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := cronScheduler.AddScheduledTask(monitoringTask); err != nil {
		log.Printf("Failed to add monitoring task: %v", err)
	} else {
		log.Println("Added scheduled monitoring task")
	}

	// 示例2: 每天上午9点生成日报
	reportTask := &scheduler.ScheduledTask{
		Name:        "每日项目报告",
		Description: "生成昨日项目进度报告",
		Type:        scheduler.TaskTypeAgent,
		CronExpr:    "0 0 9 * * 1-5", // 工作日上午9点
		Enabled:     true,
		AdapterType: "dingtalk",
		Channel:     "project_team",
		Content:     "分析昨天的Git提交、Issue、PR，生成项目进度报告",
		Agent:       "report_generator",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := cronScheduler.AddScheduledTask(reportTask); err != nil {
		log.Printf("Failed to add report task: %v", err)
	} else {
		log.Println("Added scheduled report task")
	}

	// 示例3: 每30分钟执行一次健康检查
	healthCheckTask := &scheduler.ScheduledTask{
		Name:        "系统健康检查",
		Description: "检查各服务状态",
		Type:        scheduler.TaskTypeScript,
		CronExpr:    "0 */30 * * * *", // 每30分钟
		Enabled:     false,            // 默认禁用
		AdapterType: "dingtalk",
		Channel:     "ops_team",
		ScriptPath:  "./scripts/health_check.sh",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := cronScheduler.AddScheduledTask(healthCheckTask); err != nil {
		log.Printf("Failed to add health check task: %v", err)
	} else {
		log.Println("Added scheduled health check task (disabled)")
	}
}

// loadConfig 加载配置（简化版）
func loadConfig() *Config {
	return &Config{
		OpenCode: OpenCodeConfig{
			Endpoint: os.Getenv("OPENCODE_ENDPOINT"),
			APIKey:   os.Getenv("OPENCODE_API_KEY"),
			WorkDir:  os.Getenv("OPENCODE_WORKDIR"),
		},
		DingTalk: DingTalkConfig{
			ClientID:     os.Getenv("DINGTALK_CLIENT_ID"),
			ClientSecret: os.Getenv("DINGTALK_CLIENT_SECRET"),
		},
	}
}

type Config struct {
	OpenCode OpenCodeConfig
	DingTalk DingTalkConfig
}

type OpenCodeConfig struct {
	Endpoint string
	APIKey   string
	WorkDir  string
}

type DingTalkConfig struct {
	ClientID     string
	ClientSecret string
}
