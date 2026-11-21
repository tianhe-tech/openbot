package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/user/opencode-gateway/internal/opencode"
)

// CronScheduler 定时任务调度器
type CronScheduler struct {
	cron           *cron.Cron
	taskScheduler  *TaskScheduler
	scheduledTasks map[string]*ScheduledTask // taskID -> task
	tasksMu        sync.RWMutex
	cronIDs        map[string]cron.EntryID // taskID -> cron entry ID
	cronIDsMu      sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	parser         cron.Parser // cron表达式解析器
	storagePath    string      // 持久化存储路径
	persistenceMu  sync.Mutex
	inFlight       map[string]bool
}

// NewCronScheduler 创建定时任务调度器
func NewCronScheduler(taskScheduler *TaskScheduler) *CronScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建支持6字段（秒级）和5字段的cron解析器
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	return &CronScheduler{
		cron:           cron.New(cron.WithSeconds()), // 支持秒级精度
		taskScheduler:  taskScheduler,
		scheduledTasks: make(map[string]*ScheduledTask),
		cronIDs:        make(map[string]cron.EntryID),
		ctx:            ctx,
		cancel:         cancel,
		parser:         parser,
		storagePath:    "", // 默认不持久化，需要通过 SetStoragePath 设置
		inFlight:       make(map[string]bool),
	}
}

// SetStoragePath 设置持久化存储路径
func (cs *CronScheduler) SetStoragePath(path string) {
	cs.storagePath = path
	// 确保目录存在
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("cron-scheduler: failed to create storage directory: %v", err)
	}
}

// Start 启动定时任务调度器
func (cs *CronScheduler) Start() error {
	log.Println("cron-scheduler: starting cron scheduler")

	// 加载持久化的任务
	if cs.storagePath != "" {
		if err := cs.loadTasks(); err != nil {
			return fmt.Errorf("cron-scheduler: load tasks: %w", err)
		}
	}

	cs.cron.Start()
	log.Println("cron-scheduler: cron scheduler started")
	return nil
}

// Stop 停止定时任务调度器
func (cs *CronScheduler) Stop() error {
	log.Println("cron-scheduler: stopping cron scheduler")

	cs.cancel()
	ctx := cs.cron.Stop()
	<-ctx.Done()
	if err := cs.saveTasks(); err != nil {
		return fmt.Errorf("cron-scheduler: save tasks on stop: %w", err)
	}
	log.Println("cron-scheduler: cron scheduler stopped")
	return nil
}

// AddSystemJob registers a raw cron function that is NOT persisted to storage
// and does NOT appear in user-facing task lists. Intended for internal system
// jobs (e.g., off-peak retry queue). cronExpr must be a valid 5-field cron
// expression (minute-level precision). Returns an error if the expression is
// invalid.
func (cs *CronScheduler) AddSystemJob(cronExpr string, fn func()) error {
	if _, err := cs.parser.Parse(cronExpr); err != nil {
		return fmt.Errorf("cron-scheduler: invalid system job expr %q: %w", cronExpr, err)
	}
	if _, err := cs.cron.AddFunc(cronExpr, fn); err != nil {
		return fmt.Errorf("cron-scheduler: add system job: %w", err)
	}
	return nil
}

// saveTasks 保存任务到文件
func (cs *CronScheduler) saveTasks() error {
	cs.persistenceMu.Lock()
	defer cs.persistenceMu.Unlock()
	if cs.storagePath == "" {
		return nil
	}
	cs.tasksMu.RLock()
	tasks := make([]*ScheduledTask, 0, len(cs.scheduledTasks))
	for _, task := range cs.scheduledTasks {
		tasks = append(tasks, task)
	}
	data, err := json.MarshalIndent(tasks, "", "  ")
	cs.tasksMu.RUnlock()
	if err != nil {
		return fmt.Errorf("marshal tasks: %w", err)
	}

	// 写入临时文件，然后重命名（原子操作）
	tmpPath := cs.storagePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write tasks file: %w", err)
	}

	err = os.Rename(tmpPath, cs.storagePath)
	// Windows can briefly retain handles on a replaced file. Never delete the
	// last committed snapshot to work around a sharing/permission failure.
	for attempt := 0; err != nil && runtime.GOOS == "windows" && os.IsPermission(err) && attempt < 4; attempt++ {
		log.Printf("cron-scheduler: retrying tasks file replacement: %v", err)
		time.Sleep(time.Duration(1<<attempt) * 10 * time.Millisecond)
		err = os.Rename(tmpPath, cs.storagePath)
	}
	if err != nil {
		return fmt.Errorf("rename tasks file: %w", err)
	}

	log.Printf("cron-scheduler: saved %d tasks to %s", len(tasks), cs.storagePath)
	return nil
}

// loadTasks 从文件加载任务
func (cs *CronScheduler) loadTasks() error {
	data, err := os.ReadFile(cs.storagePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("cron-scheduler: no existing tasks file at %s", cs.storagePath)
			return nil
		}
		return fmt.Errorf("read tasks file: %w", err)
	}

	var tasks []*ScheduledTask
	if err := json.Unmarshal(data, &tasks); err != nil {
		return fmt.Errorf("unmarshal tasks: %w", err)
	}

	loadedCount := 0
	for _, task := range tasks {
		if task == nil {
			return fmt.Errorf("tasks file contains a null task")
		}
		if err := normalizeScheduledOwner(task); err != nil {
			task.Enabled = false
			task.LastRunStatus = TaskStatusFailed
			task.LastRunResult = err.Error()
			log.Printf("cron-scheduler: disabled legacy task %s: %v", task.ID, err)
		}
		// 重新计算下次运行时间
		schedule, err := cs.parser.Parse(task.CronExpr)
		if err != nil {
			log.Printf("cron-scheduler: invalid cron expression for task %s: %v", task.ID, err)
			continue
		}
		nextRun := schedule.Next(time.Now())
		task.NextRunTime = &nextRun
		verifiedSessionID := ""
		if task.SessionID != "" && cs.taskScheduler != nil && cs.taskScheduler.opencodeClient != nil {
			ctx, cancel := context.WithTimeout(cs.ctx, 5*time.Second)
			resolved, resolveErr := cs.taskScheduler.opencodeClient.ResolveOrCreateSession(ctx, opencode.MessagePayload{
				Channel: task.AdapterType, UserID: task.UserID,
				ThreadID: opencode.CronThreadPrefix + task.ID, SessionID: task.SessionID,
			})
			cancel()
			if resolveErr != nil {
				task.Enabled = false
				task.LastRunStatus = TaskStatusFailed
				task.LastRunResult = resolveErr.Error()
				log.Printf("cron-scheduler: disabled task %s with unverified session %s: %v", task.ID, task.SessionID, resolveErr)
			} else {
				task.SessionID = resolved
				verifiedSessionID = resolved
			}
		}

		// 保存到内存
		cs.tasksMu.Lock()
		cs.scheduledTasks[task.ID] = cloneScheduledTask(task)
		cs.tasksMu.Unlock()
		if verifiedSessionID != "" {
			cs.taskScheduler.registerCronSession(&Task{
				ID: task.ID, Type: TaskTypeCron, AdapterType: task.AdapterType,
				UserID: task.UserID, Metadata: task.Metadata,
			}, verifiedSessionID)
		}

		// 如果任务启用，添加到cron调度器
		if task.Enabled {
			if err := cs.enableTask(task.ID); err != nil {
				log.Printf("cron-scheduler: failed to enable task %s: %v", task.ID, err)
			}
		}
		loadedCount++
	}

	log.Printf("cron-scheduler: loaded %d tasks from %s", loadedCount, cs.storagePath)
	return cs.saveTasks()
}

// AddScheduledTask 添加定时任务
func (cs *CronScheduler) AddScheduledTask(task *ScheduledTask) error {
	if task == nil {
		return fmt.Errorf("scheduled task is required")
	}
	if err := normalizeScheduledOwner(task); err != nil {
		return err
	}
	if task.ID == "" {
		task.ID = fmt.Sprintf("cron-%d", time.Now().UnixNano())
	}

	if task.CronExpr == "" {
		return fmt.Errorf("cron expression is required")
	}

	// 验证cron表达式
	if _, err := cs.parser.Parse(task.CronExpr); err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	// 计算下次运行时间
	schedule, _ := cs.parser.Parse(task.CronExpr)
	nextRun := schedule.Next(time.Now())
	task.NextRunTime = &nextRun

	// 保存任务
	cs.tasksMu.Lock()
	if _, exists := cs.scheduledTasks[task.ID]; exists {
		cs.tasksMu.Unlock()
		return fmt.Errorf("task already exists: %s", task.ID)
	}
	cs.scheduledTasks[task.ID] = cloneScheduledTask(task)
	cs.tasksMu.Unlock()

	// 如果启用，添加到cron
	if task.Enabled {
		if err := cs.enableTask(task.ID); err != nil {
			return fmt.Errorf("failed to enable task: %w", err)
		}
	}

	log.Printf("cron-scheduler: added scheduled task '%s' (cron: %s, enabled: %t)",
		task.Name, task.CronExpr, task.Enabled)

	// 自动保存
	cs.autoSave()

	return nil
}

// RemoveScheduledTask 删除定时任务
func (cs *CronScheduler) RemoveScheduledTask(taskID string) error {
	cs.tasksMu.Lock()
	task, ok := cs.scheduledTasks[taskID]
	if !ok {
		cs.tasksMu.Unlock()
		return fmt.Errorf("task not found: %s", taskID)
	}
	if cs.inFlight[taskID] {
		cs.tasksMu.Unlock()
		return fmt.Errorf("task is running: %s", taskID)
	}
	delete(cs.scheduledTasks, taskID)
	cs.tasksMu.Unlock()

	// 从cron中移除
	cs.cronIDsMu.Lock()
	if entryID, ok := cs.cronIDs[taskID]; ok {
		cs.cron.Remove(entryID)
		delete(cs.cronIDs, taskID)
	}
	cs.cronIDsMu.Unlock()

	log.Printf("cron-scheduler: removed scheduled task '%s'", task.Name)

	// 自动保存
	cs.autoSave()

	return nil
}

// UpdateScheduledTask 更新定时任务
func (cs *CronScheduler) UpdateScheduledTask(task *ScheduledTask) error {
	if task == nil {
		return fmt.Errorf("scheduled task is required")
	}
	if err := normalizeScheduledOwner(task); err != nil {
		return err
	}
	if _, err := cs.parser.Parse(task.CronExpr); err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}
	cs.tasksMu.Lock()
	oldTask, ok := cs.scheduledTasks[task.ID]
	if !ok {
		cs.tasksMu.Unlock()
		return fmt.Errorf("task not found: %s", task.ID)
	}
	if cs.inFlight[task.ID] {
		cs.tasksMu.Unlock()
		return fmt.Errorf("task is running: %s", task.ID)
	}
	needsReschedule := oldTask.CronExpr != task.CronExpr || oldTask.Enabled != task.Enabled
	task.SessionID = oldTask.SessionID
	task.UpdatedAt = time.Now()
	cs.scheduledTasks[task.ID] = cloneScheduledTask(task)
	cs.tasksMu.Unlock()

	if needsReschedule {
		// 先禁用
		cs.disableTask(task.ID)

		// 如果新任务启用，重新启用
		if task.Enabled {
			if err := cs.enableTask(task.ID); err != nil {
				return fmt.Errorf("failed to re-enable task: %w", err)
			}
		}
	}

	log.Printf("cron-scheduler: updated scheduled task '%s'", task.Name)

	// 自动保存
	cs.autoSave()

	return nil
}

// EnableTask 启用定时任务
func (cs *CronScheduler) EnableTask(taskID string) error {
	cs.tasksMu.Lock()
	task, ok := cs.scheduledTasks[taskID]
	if !ok {
		cs.tasksMu.Unlock()
		return fmt.Errorf("task not found: %s", taskID)
	}
	if err := normalizeScheduledOwner(task); err != nil {
		cs.tasksMu.Unlock()
		return err
	}
	task.Enabled = true
	task.UpdatedAt = time.Now()
	cs.tasksMu.Unlock()

	if err := cs.enableTask(taskID); err != nil {
		return err
	}

	cs.autoSave()
	return nil
}

// DisableTask 禁用定时任务
func (cs *CronScheduler) DisableTask(taskID string) error {
	cs.tasksMu.Lock()
	task, ok := cs.scheduledTasks[taskID]
	if !ok {
		cs.tasksMu.Unlock()
		return fmt.Errorf("task not found: %s", taskID)
	}
	task.Enabled = false
	task.UpdatedAt = time.Now()
	cs.tasksMu.Unlock()

	cs.disableTask(taskID)
	cs.autoSave()
	return nil
}

// enableTask 启用任务（内部方法）
func (cs *CronScheduler) enableTask(taskID string) error {
	task, err := cs.GetScheduledTask(taskID)
	if err != nil {
		return err
	}
	if err := normalizeScheduledOwner(task); err != nil {
		return err
	}

	// 创建任务执行函数
	job := func() {
		cs.executeScheduledTask(task.ID)
	}

	// 添加到cron
	cs.cronIDsMu.Lock()
	defer cs.cronIDsMu.Unlock()
	if _, exists := cs.cronIDs[taskID]; exists {
		return nil
	}
	entryID, err := cs.cron.AddFunc(task.CronExpr, job)
	if err != nil {
		return fmt.Errorf("failed to add cron job: %w", err)
	}

	cs.cronIDs[taskID] = entryID

	log.Printf("cron-scheduler: enabled task '%s' (cron: %s)", task.Name, task.CronExpr)
	return nil
}

// disableTask 禁用任务（内部方法）
func (cs *CronScheduler) disableTask(taskID string) {
	cs.cronIDsMu.Lock()
	defer cs.cronIDsMu.Unlock()

	if entryID, ok := cs.cronIDs[taskID]; ok {
		cs.cron.Remove(entryID)
		delete(cs.cronIDs, taskID)
		log.Printf("cron-scheduler: disabled task %s", taskID)
	}
}

// executeScheduledTask 执行定时任务
func (cs *CronScheduler) executeScheduledTask(taskID string) {
	if cs.ctx.Err() != nil {
		return
	}
	cs.tasksMu.Lock()
	scheduledTask, ok := cs.scheduledTasks[taskID]
	if !ok || !scheduledTask.Enabled {
		cs.tasksMu.Unlock()
		return
	}
	if cs.inFlight[taskID] {
		cs.tasksMu.Unlock()
		log.Printf("cron-scheduler: task %s skipped: already_running", taskID)
		return
	}
	if err := normalizeScheduledOwner(scheduledTask); err != nil {
		cs.tasksMu.Unlock()
		log.Printf("cron-scheduler: invalid task %s: %v", taskID, err)
		return
	}
	cs.inFlight[taskID] = true
	now := time.Now()
	scheduledTask.LastRunTime = &now
	scheduledTask.RunCount++
	scheduledTask = cloneScheduledTask(scheduledTask)
	cs.tasksMu.Unlock()

	log.Printf("cron-scheduler: executing scheduled task '%s' (type: %s, run count: %d)",
		scheduledTask.Name, scheduledTask.Type, scheduledTask.RunCount)

	task := &Task{
		ID:          fmt.Sprintf("%s-run-%d", scheduledTask.ID, scheduledTask.RunCount),
		Type:        TaskTypeCron, // 标记为定时任务类型
		Priority:    PriorityNormal,
		Status:      TaskStatusPending,
		AdapterType: scheduledTask.AdapterType,
		UserID:      scheduledTask.UserID,
		Channel:     scheduledTask.Channel,
		ThreadID:    opencode.CronThreadPrefix + scheduledTask.ID,
		SessionID:   scheduledTask.SessionID,
		Content:     scheduledTask.Content,
		Agent:       scheduledTask.Agent,
		ScriptPath:  scheduledTask.ScriptPath,
		Metadata:    make(map[string]interface{}),
		CreatedAt:   now,
		MaxRetries:  3,
	}

	// 复制并添加metadata
	for k, v := range scheduledTask.Metadata {
		task.Metadata[k] = v
	}
	task.Metadata["name"] = scheduledTask.Name // 确保任务名称在metadata中
	task.Metadata["scheduled_task_id"] = scheduledTask.ID

	if cs.taskScheduler == nil || cs.taskScheduler.opencodeClient == nil {
		cs.updateTaskStatus(taskID, TaskStatusFailed, "opencode task scheduler is not configured")
		return
	}
	cs.taskScheduler.RegisterCallback(task.ID, func(ctx context.Context, result *TaskResult) error {
		body := result.Result
		if result.Error != "" {
			body = result.Error
		}
		cs.updateTaskStatus(taskID, result.Status, body)
		return nil
	})
	task.OnSessionResolved = func(ctx context.Context, sessionID string) error {
		task.SessionID = sessionID
		cs.tasksMu.Lock()
		current, exists := cs.scheduledTasks[taskID]
		if exists {
			current.SessionID = sessionID
			current.UpdatedAt = time.Now()
		}
		cs.tasksMu.Unlock()
		if !exists {
			return fmt.Errorf("scheduled task %s was removed", taskID)
		}
		if err := cs.saveTasks(); err != nil {
			return err
		}
		cs.taskScheduler.registerCronSession(task, sessionID)
		return nil
	}
	ctx, cancel := context.WithTimeout(cs.ctx, 30*time.Second)
	_, err := cs.taskScheduler.opencodeClient.ResolveOrCreateSession(ctx, opencode.MessagePayload{
		Channel: task.AdapterType, UserID: task.UserID, ThreadID: task.ThreadID,
		SessionID: task.SessionID, OnSessionResolved: task.OnSessionResolved,
	})
	cancel()
	if err == nil {
		err = cs.taskScheduler.SubmitTask(task)
	}
	if err != nil {
		log.Printf("cron-scheduler: task %s preparation/submission failed: %v", taskID, err)
		task.Status, task.Error = TaskStatusFailed, err.Error()
		cs.taskScheduler.finishTask(task)
	}
}

// updateTaskStatus 更新定时任务状态
func (cs *CronScheduler) updateTaskStatus(taskID string, status TaskStatus, result string) {
	cs.tasksMu.Lock()
	delete(cs.inFlight, taskID)
	if task, ok := cs.scheduledTasks[taskID]; ok {
		task.LastRunStatus = status
		task.LastRunResult = result
		task.UpdatedAt = time.Now()
	}
	cs.tasksMu.Unlock()
	cs.autoSave()
}

// GetScheduledTask 获取定时任务
func (cs *CronScheduler) GetScheduledTask(taskID string) (*ScheduledTask, error) {
	cs.tasksMu.RLock()
	defer cs.tasksMu.RUnlock()

	task, ok := cs.scheduledTasks[taskID]
	if !ok {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	return cloneScheduledTask(task), nil
}

// ListScheduledTasks 列出所有定时任务
func (cs *CronScheduler) ListScheduledTasks() []*ScheduledTask {
	cs.tasksMu.RLock()
	defer cs.tasksMu.RUnlock()

	tasks := make([]*ScheduledTask, 0, len(cs.scheduledTasks))
	for _, task := range cs.scheduledTasks {
		tasks = append(tasks, cloneScheduledTask(task))
	}

	return tasks
}

// GetScheduledTasksByAdapter 获取指定adapter的定时任务
func (cs *CronScheduler) GetScheduledTasksByAdapter(adapterType string) []*ScheduledTask {
	cs.tasksMu.RLock()
	defer cs.tasksMu.RUnlock()

	tasks := make([]*ScheduledTask, 0)
	for _, task := range cs.scheduledTasks {
		if task.AdapterType == adapterType {
			tasks = append(tasks, cloneScheduledTask(task))
		}
	}

	return tasks
}

// GetStats 获取统计信息
func (cs *CronScheduler) GetStats() map[string]interface{} {
	cs.tasksMu.RLock()
	totalTasks := len(cs.scheduledTasks)
	enabledCount := 0
	for _, task := range cs.scheduledTasks {
		if task.Enabled {
			enabledCount++
		}
	}
	cs.tasksMu.RUnlock()

	return map[string]interface{}{
		"total_scheduled_tasks": totalTasks,
		"enabled_tasks":         enabledCount,
		"disabled_tasks":        totalTasks - enabledCount,
		"cron_entries":          len(cs.cron.Entries()),
	}
}

// autoSave 自动保存任务（如果配置了存储路径）
func (cs *CronScheduler) autoSave() {
	if cs.storagePath == "" {
		return
	}
	if err := cs.saveTasks(); err != nil {
		log.Printf("cron-scheduler: auto-save failed: %v", err)
	}
}

func normalizeScheduledOwner(task *ScheduledTask) error {
	task.UserID = strings.TrimSpace(task.UserID)
	if task.UserID == "" {
		if creator, ok := task.Metadata["created_by"].(string); ok {
			task.UserID = strings.TrimSpace(creator)
		}
	}
	if strings.TrimSpace(task.AdapterType) == "" || task.UserID == "" {
		return fmt.Errorf("cron task %s requires an adapter and an unambiguous owner", task.ID)
	}
	if task.Channel == "" && task.AdapterType == "feishu" {
		task.Channel, _ = task.Metadata["receive_id"].(string)
	}
	return nil
}

func cloneScheduledTask(task *ScheduledTask) *ScheduledTask {
	copy := *task
	copy.Metadata = maps.Clone(task.Metadata)
	return &copy
}
