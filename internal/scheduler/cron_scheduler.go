package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
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
			log.Printf("cron-scheduler: failed to load tasks: %v", err)
		}
	}

	cs.cron.Start()
	log.Println("cron-scheduler: cron scheduler started")
	return nil
}

// Stop 停止定时任务调度器
func (cs *CronScheduler) Stop() error {
	log.Println("cron-scheduler: stopping cron scheduler")

	// 保存任务到持久化存储
	if cs.storagePath != "" {
		if err := cs.saveTasks(); err != nil {
			log.Printf("cron-scheduler: failed to save tasks: %v", err)
		}
	}

	cs.cancel()
	ctx := cs.cron.Stop()
	<-ctx.Done()
	log.Println("cron-scheduler: cron scheduler stopped")
	return nil
}

// saveTasks 保存任务到文件
func (cs *CronScheduler) saveTasks() error {
	cs.tasksMu.RLock()
	tasks := make([]*ScheduledTask, 0, len(cs.scheduledTasks))
	for _, task := range cs.scheduledTasks {
		tasks = append(tasks, task)
	}
	cs.tasksMu.RUnlock()

	data, err := json.MarshalIndent(tasks, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal tasks: %w", err)
	}

	// 写入临时文件，然后重命名（原子操作）
	tmpPath := cs.storagePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write tasks file: %w", err)
	}

	if err := os.Rename(tmpPath, cs.storagePath); err != nil {
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
		// 重新计算下次运行时间
		schedule, err := cs.parser.Parse(task.CronExpr)
		if err != nil {
			log.Printf("cron-scheduler: invalid cron expression for task %s: %v", task.ID, err)
			continue
		}
		nextRun := schedule.Next(time.Now())
		task.NextRunTime = &nextRun

		// 保存到内存
		cs.scheduledTasks[task.ID] = task

		// 如果任务启用，添加到cron调度器
		if task.Enabled {
			if err := cs.enableTask(task.ID); err != nil {
				log.Printf("cron-scheduler: failed to enable task %s: %v", task.ID, err)
			}
		}
		loadedCount++
	}

	log.Printf("cron-scheduler: loaded %d tasks from %s", loadedCount, cs.storagePath)
	return nil
}

// AddScheduledTask 添加定时任务
func (cs *CronScheduler) AddScheduledTask(task *ScheduledTask) error {
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
	cs.scheduledTasks[task.ID] = task
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
	cs.tasksMu.RLock()
	oldTask, ok := cs.scheduledTasks[task.ID]
	cs.tasksMu.RUnlock()

	if !ok {
		return fmt.Errorf("task not found: %s", task.ID)
	}

	// 如果cron表达式变化，需要重新调度
	needsReschedule := oldTask.CronExpr != task.CronExpr || oldTask.Enabled != task.Enabled

	// 更新任务
	task.UpdatedAt = time.Now()
	cs.tasksMu.Lock()
	cs.scheduledTasks[task.ID] = task
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
	cs.tasksMu.RLock()
	task, ok := cs.scheduledTasks[taskID]
	cs.tasksMu.RUnlock()

	if !ok {
		return fmt.Errorf("task not found: %s", taskID)
	}

	// 创建任务执行函数
	job := func() {
		cs.executeScheduledTask(task.ID)
	}

	// 添加到cron
	entryID, err := cs.cron.AddFunc(task.CronExpr, job)
	if err != nil {
		return fmt.Errorf("failed to add cron job: %w", err)
	}

	cs.cronIDsMu.Lock()
	cs.cronIDs[taskID] = entryID
	cs.cronIDsMu.Unlock()

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
	cs.tasksMu.Lock()
	scheduledTask, ok := cs.scheduledTasks[taskID]
	if !ok {
		cs.tasksMu.Unlock()
		log.Printf("cron-scheduler: task %s not found during execution", taskID)
		return
	}

	// 更新运行信息
	now := time.Now()
	scheduledTask.LastRunTime = &now
	scheduledTask.RunCount++

	cs.tasksMu.Unlock()

	log.Printf("cron-scheduler: executing scheduled task '%s' (type: %s, run count: %d)",
		scheduledTask.Name, scheduledTask.Type, scheduledTask.RunCount)

	// 创建执行任务
	task := &Task{
		ID:          fmt.Sprintf("%s-run-%d", scheduledTask.ID, scheduledTask.RunCount),
		Type:        TaskTypeCron, // 标记为定时任务类型
		Priority:    PriorityNormal,
		Status:      TaskStatusPending,
		AdapterType: scheduledTask.AdapterType,
		Channel:     scheduledTask.Channel,
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

	// 提交任务到调度器
	if err := cs.taskScheduler.SubmitTask(task); err != nil {
		log.Printf("cron-scheduler: failed to submit task for '%s': %v", scheduledTask.Name, err)
		cs.updateTaskStatus(taskID, TaskStatusFailed, err.Error())
		return
	}

	// 注册回调以更新状态
	cs.taskScheduler.RegisterCallback(task.ID, func(ctx context.Context, result *TaskResult) error {
		cs.updateTaskStatus(taskID, result.Status, result.Result)
		return nil
	})
}

// updateTaskStatus 更新定时任务状态
func (cs *CronScheduler) updateTaskStatus(taskID string, status TaskStatus, result string) {
	cs.tasksMu.Lock()
	defer cs.tasksMu.Unlock()

	if task, ok := cs.scheduledTasks[taskID]; ok {
		task.LastRunStatus = status
		task.LastRunResult = result
		task.UpdatedAt = time.Now()
	}
}

// GetScheduledTask 获取定时任务
func (cs *CronScheduler) GetScheduledTask(taskID string) (*ScheduledTask, error) {
	cs.tasksMu.RLock()
	defer cs.tasksMu.RUnlock()

	task, ok := cs.scheduledTasks[taskID]
	if !ok {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	return task, nil
}

// ListScheduledTasks 列出所有定时任务
func (cs *CronScheduler) ListScheduledTasks() []*ScheduledTask {
	cs.tasksMu.RLock()
	defer cs.tasksMu.RUnlock()

	tasks := make([]*ScheduledTask, 0, len(cs.scheduledTasks))
	for _, task := range cs.scheduledTasks {
		tasks = append(tasks, task)
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
			tasks = append(tasks, task)
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
