package scheduler

import (
	"context"
	"fmt"
	"time"
)

// TaskType 定义任务类型
type TaskType string

const (
	TaskTypeMessage    TaskType = "message"    // 普通消息任务
	TaskTypeScript     TaskType = "script"     // 执行脚本
	TaskTypeAgent      TaskType = "agent"      // 智能体流程
	TaskTypeCron       TaskType = "cron"       // 定时任务
	TaskTypeMonitoring TaskType = "monitoring" // 监控任务（如社交媒体监控）
)

// TaskStatus 任务状态
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"   // 等待执行
	TaskStatusRunning   TaskStatus = "running"   // 执行中
	TaskStatusCompleted TaskStatus = "completed" // 已完成
	TaskStatusFailed    TaskStatus = "failed"    // 失败
	TaskStatusCanceled  TaskStatus = "canceled"  // 已取消
)

// TaskPriority 任务优先级
type TaskPriority int

const (
	PriorityLow    TaskPriority = 0
	PriorityNormal TaskPriority = 5
	PriorityHigh   TaskPriority = 10
	PriorityUrgent TaskPriority = 15
)

// Task 表示一个待执行的任务
type Task struct {
	ID          string                 `json:"id"`
	Type        TaskType               `json:"type"`
	Priority    TaskPriority           `json:"priority"`
	Status      TaskStatus             `json:"status"`
	AdapterType string                 `json:"adapter_type"` // 来源adapter类型（dingtalk/wecom/feishu）
	UserID      string                 `json:"user_id"`
	Channel     string                 `json:"channel"`     // 用于回复的channel
	ThreadID    string                 `json:"thread_id"`   // 会话ID
	SessionID   string                 `json:"session_id"`  // OpenCode session ID
	Content     string                 `json:"content"`     // 任务内容
	Agent       string                 `json:"agent"`       // 指定的agent名称
	ScriptPath  string                 `json:"script_path"` // 脚本路径（TaskTypeScript）
	Metadata    map[string]interface{} `json:"metadata"`    // 额外元数据
	CreatedAt   time.Time              `json:"created_at"`
	StartedAt   *time.Time             `json:"started_at"`
	CompletedAt *time.Time             `json:"completed_at"`
	Result      string                 `json:"result"`      // 执行结果
	Error       string                 `json:"error"`       // 错误信息
	RetryCount  int                    `json:"retry_count"` // 重试次数
	MaxRetries  int                    `json:"max_retries"` // 最大重试次数
}

// ScheduledTask 定时任务
type ScheduledTask struct {
	ID            string                 `json:"id"`
	Name          string                 `json:"name"`
	Description   string                 `json:"description"`
	Type          TaskType               `json:"type"`
	CronExpr      string                 `json:"cron_expr"`       // Cron表达式
	Enabled       bool                   `json:"enabled"`         // 是否启用
	AdapterType   string                 `json:"adapter_type"`    // 发送结果到哪个adapter
	Channel       string                 `json:"channel"`         // 发送到哪个channel
	Content       string                 `json:"content"`         // 固定内容或提示词
	Agent         string                 `json:"agent"`           // 使用的agent
	ScriptPath    string                 `json:"script_path"`     // 脚本路径
	Metadata      map[string]interface{} `json:"metadata"`        // 额外配置
	LastRunTime   *time.Time             `json:"last_run_time"`   // 上次运行时间
	NextRunTime   *time.Time             `json:"next_run_time"`   // 下次运行时间
	LastRunStatus TaskStatus             `json:"last_run_status"` // 上次运行状态
	LastRunResult string                 `json:"last_run_result"` // 上次运行结果
	RunCount      int                    `json:"run_count"`       // 运行次数
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
}

// TaskResult 任务执行结果
type TaskResult struct {
	TaskID      string     `json:"task_id"`
	Status      TaskStatus `json:"status"`
	Result      string     `json:"result"`
	Error       string     `json:"error"`
	SessionID   string     `json:"session_id"`
	UserID      string     `json:"user_id"` // 用户ID，用于建立session映射
	CompletedAt time.Time  `json:"completed_at"`
}

// TaskCallback 任务完成时的回调函数
type TaskCallback func(ctx context.Context, result *TaskResult) error

// NewTask 创建新任务
func NewTask(taskType TaskType, adapterType, userID, content string) *Task {
	now := time.Now()
	return &Task{
		ID:          fmt.Sprintf("%s-%d", taskType, now.UnixNano()),
		Type:        taskType,
		Priority:    PriorityNormal,
		Status:      TaskStatusPending,
		AdapterType: adapterType,
		UserID:      userID,
		Content:     content,
		Metadata:    make(map[string]interface{}),
		CreatedAt:   now,
		MaxRetries:  3,
	}
}

// CanRetry 判断任务是否可以重试
func (t *Task) CanRetry() bool {
	return t.RetryCount < t.MaxRetries
}

// IncrementRetry 增加重试次数
func (t *Task) IncrementRetry() {
	t.RetryCount++
}
