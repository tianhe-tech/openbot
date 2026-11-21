package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/user/opencode-gateway/internal/opencode"
)

// TaskSchedulerConfig 任务调度器配置
type TaskSchedulerConfig struct {
	MaxConcurrentTasks int           // 最大并发任务数
	MaxQueueSize       int           // 最大队列长度
	SessionPoolSize    int           // Session池大小
	TaskTimeout        time.Duration // 任务超时时间
	CleanupInterval    time.Duration // 清理间隔
}

// DefaultTaskSchedulerConfig 默认配置
func DefaultTaskSchedulerConfig() TaskSchedulerConfig {
	return TaskSchedulerConfig{
		MaxConcurrentTasks: 10,
		MaxQueueSize:       1000,
		SessionPoolSize:    20,
		TaskTimeout:        30 * time.Minute,
		CleanupInterval:    1 * time.Hour,
	}
}

// CronSessionInfo 存储定时任务session的相关信息
type CronSessionInfo struct {
	SessionID   string
	AdapterType string
	TaskID      string
	Metadata    map[string]interface{}
	CreatedAt   time.Time
}

// TaskScheduler 任务调度器
type TaskScheduler struct {
	cfg            TaskSchedulerConfig
	opencodeClient *opencode.Client
	taskQueue      chan *Task
	submitMu       sync.RWMutex
	stopped        bool
	activeTasks    map[string]*Task
	activeTasksMu  sync.RWMutex
	taskHistory    []*Task // 任务历史（最近的N个）
	taskHistoryMu  sync.RWMutex
	maxHistorySize int
	callbacks      map[string][]TaskCallback // taskID -> callbacks
	callbacksMu    sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	adapters       map[string]AdapterSender // adapterType -> sender
	adaptersMu     sync.RWMutex
	cronSessions   sync.Map // map[sessionID]*CronSessionInfo 定时任务session映射
}

// AdapterSender 定义adapter发送消息的接口
type AdapterSender interface {
	SendMessage(ctx context.Context, channel, userID, content string) error
}

// SessionRegistrar 是可选接口，adapter可实现此接口以支持
// 定时任务的session注册，使事件能正确路由到adapter
type SessionRegistrar interface {
	RegisterCronSession(sessionID string, metadata map[string]interface{})
}

// NewTaskScheduler 创建任务调度器
func NewTaskScheduler(client *opencode.Client, cfg TaskSchedulerConfig) *TaskScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	return &TaskScheduler{
		cfg:            cfg,
		opencodeClient: client,
		taskQueue:      make(chan *Task, cfg.MaxQueueSize),
		activeTasks:    make(map[string]*Task),
		taskHistory:    make([]*Task, 0, 100),
		maxHistorySize: 100,
		callbacks:      make(map[string][]TaskCallback),
		adapters:       make(map[string]AdapterSender),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// RegisterAdapter 注册adapter
func (s *TaskScheduler) RegisterAdapter(adapterType string, sender AdapterSender) {
	s.adaptersMu.Lock()
	defer s.adaptersMu.Unlock()
	s.adapters[adapterType] = sender
	log.Printf("scheduler: registered adapter '%s'", adapterType)
}

// Start 启动任务调度器
func (s *TaskScheduler) Start() error {
	log.Printf("scheduler: starting task scheduler (max concurrent: %d, queue size: %d)",
		s.cfg.MaxConcurrentTasks, s.cfg.MaxQueueSize)

	// 启动工作协程池
	for i := 0; i < s.cfg.MaxConcurrentTasks; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}

	// 启动清理协程
	s.wg.Add(1)
	go s.cleanupWorker()

	log.Println("scheduler: task scheduler started")
	return nil
}

// Stop 停止任务调度器
func (s *TaskScheduler) Stop() error {
	log.Println("scheduler: stopping task scheduler...")
	s.cancel()
	s.submitMu.Lock()
	s.stopped = true
	s.submitMu.Unlock()
	s.wg.Wait()
	for {
		select {
		case task := <-s.taskQueue:
			task.Status = TaskStatusCanceled
			task.Error = s.ctx.Err().Error()
			task.Result = ""
			s.finishTask(task)
			s.addToHistory(task)
		default:
			log.Println("scheduler: task scheduler stopped")
			return nil
		}
	}
}

// SubmitTask 提交任务
func (s *TaskScheduler) SubmitTask(task *Task) error {
	s.submitMu.RLock()
	defer s.submitMu.RUnlock()
	if s.stopped {
		return fmt.Errorf("scheduler stopped: %w", context.Canceled)
	}
	select {
	case <-s.ctx.Done():
		return fmt.Errorf("scheduler stopped: %w", s.ctx.Err())
	case s.taskQueue <- task:
		log.Printf("scheduler: task %s submitted (type: %s, priority: %d)", task.ID, task.Type, task.Priority)
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("task queue full, submission timeout")
	}
}

// RegisterCallback 注册任务完成回调
func (s *TaskScheduler) RegisterCallback(taskID string, callback TaskCallback) {
	s.callbacksMu.Lock()
	defer s.callbacksMu.Unlock()
	s.callbacks[taskID] = append(s.callbacks[taskID], callback)
}

// worker 工作协程
func (s *TaskScheduler) worker(id int) {
	defer s.wg.Done()
	log.Printf("scheduler: worker %d started", id)

	for {
		select {
		case <-s.ctx.Done():
			return
		case task := <-s.taskQueue:
			s.executeTask(id, task)
		}
	}
}

// executeTask 执行任务
func (s *TaskScheduler) executeTask(workerID int, task *Task) {
	// 标记任务为活跃
	s.activeTasksMu.Lock()
	s.activeTasks[task.ID] = task
	s.activeTasksMu.Unlock()

	defer func() {
		// 移除活跃任务
		s.activeTasksMu.Lock()
		delete(s.activeTasks, task.ID)
		s.activeTasksMu.Unlock()

		// 添加到历史
		s.addToHistory(task)
	}()

	// 更新任务状态
	now := time.Now()
	task.Status = TaskStatusRunning
	task.StartedAt = &now

	log.Printf("scheduler: worker %d executing task %s (type: %s)", workerID, task.ID, task.Type)

	for {
		ctx, cancel := context.WithTimeout(s.ctx, s.cfg.TaskTimeout)
		var result *TaskResult
		err := ctx.Err()
		if err == nil {
			switch task.Type {
			case TaskTypeMessage, TaskTypeAgent, TaskTypeCron:
				result, err = s.executeMessageTask(ctx, task)
			case TaskTypeScript:
				result, err = s.executeScriptTask(ctx, task)
			case TaskTypeMonitoring:
				result, err = s.executeMonitoringTask(ctx, task)
			default:
				err = fmt.Errorf("unknown task type: %s", task.Type)
			}
		}
		cancel()
		if result != nil && result.SessionID != "" {
			task.SessionID = result.SessionID
		}
		task.Result = ""
		if err == nil {
			task.Status = TaskStatusCompleted
			task.Error = ""
			if result != nil {
				task.Result = result.Result
			}
			break
		}
		task.Error = err.Error()
		log.Printf("scheduler: task %s failed: %v", task.ID, err)
		if s.ctx.Err() != nil {
			task.Status = TaskStatusCanceled
			break
		}
		if !task.CanRetry() {
			task.Status = TaskStatusFailed
			break
		}
		task.IncrementRetry()
		log.Printf("scheduler: retrying task %s (attempt %d/%d)", task.ID, task.RetryCount, task.MaxRetries)
	}
	s.finishTask(task)
}

func (s *TaskScheduler) finishTask(task *Task) {
	completedAt := time.Now()
	task.CompletedAt = &completedAt
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.invokeCallbacks(ctx, &TaskResult{
		TaskID: task.ID, Status: task.Status, Result: task.Result, Error: task.Error,
		SessionID: task.SessionID, UserID: task.UserID, CompletedAt: completedAt,
	})
	shouldSend := (task.Result != "" || task.Error != "") && task.Channel != ""
	if !shouldSend && task.AdapterType == "feishu" {
		_, hasReceiveID := task.Metadata["receive_id"]
		shouldSend = hasReceiveID
	}
	if !shouldSend && task.AdapterType == "dingtalk" {
		_, hasWebhook := task.Metadata["session_webhook"]
		shouldSend = hasWebhook
	}

	if shouldSend {
		s.sendResultToAdapter(ctx, task)
	}
}

// executeMessageTask 执行消息任务
func (s *TaskScheduler) executeMessageTask(ctx context.Context, task *Task) (*TaskResult, error) {
	if s.opencodeClient == nil {
		return nil, fmt.Errorf("opencode client is not configured")
	}
	payload := opencode.MessagePayload{
		Channel:           task.AdapterType,
		UserID:            task.UserID,
		ThreadID:          task.ThreadID,
		SessionID:         task.SessionID,
		Content:           task.Content,
		Agent:             task.Agent,
		Metadata:          convertMetadata(task.Metadata),
		OnSessionResolved: task.OnSessionResolved,
	}
	if task.Type == TaskTypeCron {
		if task.OnSessionResolved == nil {
			return nil, fmt.Errorf("cron session binding hook is required")
		}
		sessionID, err := s.opencodeClient.ResolveOrCreateSession(ctx, payload)
		if sessionID != "" {
			task.SessionID = sessionID
			payload.SessionID = sessionID
		}
		if err != nil {
			return nil, err
		}
		status, err := s.opencodeClient.GetSessionStatus(ctx)
		if err != nil {
			return nil, fmt.Errorf("check cron session status: %w", err)
		}
		if state := status[sessionID].Type; state != "" && state != "idle" {
			return nil, fmt.Errorf("cron session %s is %s; refusing overlapping prompt", sessionID, state)
		}
	}
	response, err := s.opencodeClient.SendMessage(ctx, payload)

	if err != nil {
		return nil, fmt.Errorf("send message to opencode: %w", err)
	}

	return &TaskResult{
		TaskID:      task.ID,
		Status:      TaskStatusCompleted,
		Result:      response.Reply,
		SessionID:   response.SessionID,
		CompletedAt: time.Now(),
		UserID:      task.UserID,
	}, nil
}

// executeScriptTask 执行脚本任务
func (s *TaskScheduler) executeScriptTask(ctx context.Context, task *Task) (*TaskResult, error) {
	if task.ScriptPath == "" {
		return nil, fmt.Errorf("script path is required")
	}

	// 创建或获取session
	sessionID := task.SessionID
	if sessionID == "" {
		response, err := s.opencodeClient.SendMessage(ctx, opencode.MessagePayload{
			Channel:  task.AdapterType,
			UserID:   task.UserID,
			ThreadID: task.ThreadID,
			Content:  "Initialize session for script execution",
			// Bootstrap prompt: it only materializes an upstream session for
			// shell execution and is not a user request, so it must never
			// become an archived conversation turn.
			Internal: true,
		})
		if err != nil {
			return nil, fmt.Errorf("create session: %w", err)
		}
		sessionID = response.SessionID
	}

	// 执行脚本
	result, err := s.opencodeClient.ExecuteShell(ctx, sessionID, task.ScriptPath)
	if err != nil {
		return nil, fmt.Errorf("execute script: %w", err)
	}

	resultStr := fmt.Sprintf("Script executed: %s", result.ID)
	if task.Content != "" {
		resultStr = fmt.Sprintf("%s\n%s", resultStr, task.Content)
	}

	return &TaskResult{
		TaskID:      task.ID,
		Status:      TaskStatusCompleted,
		Result:      resultStr,
		SessionID:   sessionID,
		CompletedAt: time.Now(),
	}, nil
}

// executeMonitoringTask 执行监控任务
func (s *TaskScheduler) executeMonitoringTask(ctx context.Context, task *Task) (*TaskResult, error) {
	// 监控任务通常是通过智能体来实现的
	// 例如：搜索社交平台内容，检测新内容等
	response, err := s.opencodeClient.SendMessage(ctx, opencode.MessagePayload{
		Channel:  task.AdapterType,
		UserID:   task.UserID,
		ThreadID: task.ThreadID,
		Content:  task.Content,
		Agent:    task.Agent,
		Metadata: convertMetadata(task.Metadata),
	})

	if err != nil {
		return nil, fmt.Errorf("execute monitoring task: %w", err)
	}

	return &TaskResult{
		TaskID:      task.ID,
		Status:      TaskStatusCompleted,
		Result:      response.Reply,
		SessionID:   response.SessionID,
		CompletedAt: time.Now(),
		UserID:      task.UserID,
	}, nil
}

// sendResultToAdapter 发送结果到adapter
func (s *TaskScheduler) sendResultToAdapter(ctx context.Context, task *Task) {
	s.adaptersMu.RLock()
	sender, ok := s.adapters[task.AdapterType]
	s.adaptersMu.RUnlock()

	if !ok {
		log.Printf("scheduler: adapter '%s' not registered, cannot send result", task.AdapterType)
		return
	}

	// 格式化消息：添加任务标识和结果
	var message string
	if task.Type == TaskTypeCron {
		// 定时任务，添加任务ID和名称标识
		taskName := "未命名任务"
		if name, ok := task.Metadata["name"].(string); ok && name != "" {
			taskName = name
		}
		status, body := "完成", task.Result
		if task.Status != TaskStatusCompleted {
			status, body = "失败", task.Error
			if task.Status == TaskStatusCanceled {
				status = "取消"
			}
		}
		message = fmt.Sprintf("🔔 定时任务执行%s\n\n📋 任务: %s\n🆔 任务ID: %s\n⏰ 完成时间: %s\n\n📝 执行结果:\n%s",
			status,
			taskName,
			task.ID,
			time.Now().Format("2006-01-02 15:04:05"),
			body,
		)
	} else {
		// 普通任务，直接发送结果
		message = task.Result
		if task.Error != "" {
			message = "任务执行失败: " + task.Error
		}
	}

	var channel, userID string

	switch task.AdapterType {
	case "feishu":
		receiveID, hasRecvID := task.Metadata["receive_id"].(string)
		receiveIDType, hasRecvType := task.Metadata["receive_id_type"].(string)

		if !hasRecvID || receiveID == "" {
			log.Printf("scheduler: feishu - no receive_id in metadata, cannot send result")
			return
		}

		userID = receiveID
		if hasRecvType && receiveIDType != "" {
			channel = receiveIDType
		} else {
			channel = "open_id"
		}

		recvLabel := receiveID
		if len(recvLabel) > 8 {
			recvLabel = recvLabel[:8]
		}
		log.Printf("scheduler: feishu sending to receive_id=%s type=%s", recvLabel, channel)

	case "dingtalk":
		if webhook, ok := task.Metadata["session_webhook"].(string); ok && webhook != "" {
			channel = webhook
			userID = ""
			log.Printf("scheduler: dingtalk using session_webhook")
		} else {
			log.Printf("scheduler: dingtalk - no session_webhook in metadata, using fallback channel=%s", task.Channel)
			channel = task.Channel
			userID = task.UserID
		}

	default:
		log.Printf("scheduler: adapter '%s' - using default channel=%s userID=%s", task.AdapterType, task.Channel, task.UserID[:min(12, len(task.UserID))])
		channel = task.Channel
		userID = task.UserID
	}

	if channel == "" {
		log.Printf("scheduler: adapter '%s' - no channel configured, skipping result send", task.AdapterType)
		return
	}

	err := sender.SendMessage(ctx, channel, userID, message)
	if err != nil {
		log.Printf("scheduler: failed to send result to adapter '%s': %v", task.AdapterType, err)
	} else {
		log.Printf("scheduler: result sent to adapter '%s' for task %s (channel: %s)", task.AdapterType, task.ID, channel[:min(20, len(channel))])
	}
}

// invokeCallbacks 调用任务回调
func (s *TaskScheduler) invokeCallbacks(ctx context.Context, result *TaskResult) {
	s.callbacksMu.Lock()
	callbacks := s.callbacks[result.TaskID]
	delete(s.callbacks, result.TaskID)
	s.callbacksMu.Unlock()

	for _, callback := range callbacks {
		if err := callback(ctx, result); err != nil {
			log.Printf("scheduler: callback error for task %s: %v", result.TaskID, err)
		}
	}

}

// addToHistory 添加到历史
func (s *TaskScheduler) addToHistory(task *Task) {
	s.taskHistoryMu.Lock()
	defer s.taskHistoryMu.Unlock()

	s.taskHistory = append(s.taskHistory, task)
	if len(s.taskHistory) > s.maxHistorySize {
		s.taskHistory = s.taskHistory[1:]
	}
}

// cleanupWorker 清理工作协程
func (s *TaskScheduler) cleanupWorker() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.cleanup()
		}
	}
}

// cleanup 清理过期数据
func (s *TaskScheduler) cleanup() {
	now := time.Now()
	cutoff := now.Add(-24 * time.Hour)

	s.taskHistoryMu.Lock()
	defer s.taskHistoryMu.Unlock()

	// 清理24小时前的历史记录
	newHistory := make([]*Task, 0, len(s.taskHistory))
	for _, task := range s.taskHistory {
		if task.CreatedAt.After(cutoff) {
			newHistory = append(newHistory, task)
		}
	}
	s.taskHistory = newHistory

	// 清理过期的定时任务session映射
	s.CleanupCronSessions(24 * time.Hour)

	log.Printf("scheduler: cleaned up old task history, remaining: %d", len(s.taskHistory))
}

// GetTask 获取任务信息
func (s *TaskScheduler) GetTask(taskID string) (*Task, error) {
	// 先查活跃任务
	s.activeTasksMu.RLock()
	if task, ok := s.activeTasks[taskID]; ok {
		s.activeTasksMu.RUnlock()
		return task, nil
	}
	s.activeTasksMu.RUnlock()

	// 再查历史
	s.taskHistoryMu.RLock()
	defer s.taskHistoryMu.RUnlock()
	for _, task := range s.taskHistory {
		if task.ID == taskID {
			return task, nil
		}
	}

	return nil, fmt.Errorf("task not found: %s", taskID)
}

// ListActiveTasks 列出活跃任务
func (s *TaskScheduler) ListActiveTasks() []*Task {
	s.activeTasksMu.RLock()
	defer s.activeTasksMu.RUnlock()

	tasks := make([]*Task, 0, len(s.activeTasks))
	for _, task := range s.activeTasks {
		tasks = append(tasks, task)
	}
	return tasks
}

// ListTaskHistory 列出任务历史
func (s *TaskScheduler) ListTaskHistory(limit int) []*Task {
	s.taskHistoryMu.RLock()
	defer s.taskHistoryMu.RUnlock()

	if limit <= 0 || limit > len(s.taskHistory) {
		limit = len(s.taskHistory)
	}

	// 返回最近的N个
	start := len(s.taskHistory) - limit
	tasks := make([]*Task, limit)
	copy(tasks, s.taskHistory[start:])
	return tasks
}

// GetStats 获取统计信息
func (s *TaskScheduler) GetStats() map[string]interface{} {
	s.activeTasksMu.RLock()
	activeCount := len(s.activeTasks)
	s.activeTasksMu.RUnlock()

	s.taskHistoryMu.RLock()
	historyCount := len(s.taskHistory)
	s.taskHistoryMu.RUnlock()

	queuedCount := len(s.taskQueue)

	return map[string]interface{}{
		"active_tasks":        activeCount,
		"queued_tasks":        queuedCount,
		"history_count":       historyCount,
		"max_concurrent":      s.cfg.MaxConcurrentTasks,
		"max_queue_size":      s.cfg.MaxQueueSize,
		"registered_adapters": len(s.adapters),
	}
}

// registerCronSession 注册定时任务session到adapter
func (s *TaskScheduler) registerCronSession(task *Task, sessionID string) {
	// 存储session -> adapter映射
	s.cronSessions.Store(sessionID, &CronSessionInfo{
		SessionID:   sessionID,
		AdapterType: task.AdapterType,
		TaskID:      task.ID,
		Metadata:    task.Metadata,
		CreatedAt:   time.Now(),
	})

	// 如果adapter实现了SessionRegistrar接口，注册session
	s.adaptersMu.RLock()
	sender, ok := s.adapters[task.AdapterType]
	s.adaptersMu.RUnlock()

	if ok {
		if registrar, implements := sender.(SessionRegistrar); implements {
			registrar.RegisterCronSession(sessionID, task.Metadata)
			log.Printf("scheduler: registered cron session %s with adapter '%s'",
				sessionID[:min(8, len(sessionID))], task.AdapterType)
		} else {
			log.Printf("scheduler: adapter '%s' does not implement SessionRegistrar, cron session %s not registered",
				task.AdapterType, sessionID[:min(8, len(sessionID))])
		}
	}
}

// GetCronSessionInfo 获取定时任务session信息（供主事件处理器查询）
func (s *TaskScheduler) GetCronSessionInfo(sessionID string) (*CronSessionInfo, bool) {
	if val, ok := s.cronSessions.Load(sessionID); ok {
		return val.(*CronSessionInfo), true
	}
	return nil, false
}

// IsCronSession 判断是否为定时任务session
func (s *TaskScheduler) IsCronSession(sessionID string) bool {
	_, ok := s.cronSessions.Load(sessionID)
	return ok
}

// CleanupCronSessions 清理过期的定时任务session映射
func (s *TaskScheduler) CleanupCronSessions(maxAge time.Duration) {
	now := time.Now()
	s.cronSessions.Range(func(key, value interface{}) bool {
		info := value.(*CronSessionInfo)
		if now.Sub(info.CreatedAt) > maxAge {
			s.cronSessions.Delete(key)
		}
		return true
	})
}

// convertMetadata 转换metadata格式
func convertMetadata(metadata map[string]interface{}) map[string]string {
	result := make(map[string]string)
	for k, v := range metadata {
		result[k] = fmt.Sprintf("%v", v)
	}
	return result
}
