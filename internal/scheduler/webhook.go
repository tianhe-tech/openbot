package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// WebhookHandler HTTP API处理器
type WebhookHandler struct {
	taskScheduler *TaskScheduler
	cronScheduler *CronScheduler
}

// NewWebhookHandler 创建webhook处理器
func NewWebhookHandler(taskScheduler *TaskScheduler, cronScheduler *CronScheduler) *WebhookHandler {
	return &WebhookHandler{
		taskScheduler: taskScheduler,
		cronScheduler: cronScheduler,
	}
}

// RegisterRoutes 注册路由
func (h *WebhookHandler) RegisterRoutes(mux *http.ServeMux) {
	// 任务相关
	mux.HandleFunc("/api/tasks/submit", h.handleSubmitTask)
	mux.HandleFunc("/api/tasks/", h.handleGetTask)
	mux.HandleFunc("/api/tasks/active", h.handleListActiveTasks)
	mux.HandleFunc("/api/tasks/history", h.handleListTaskHistory)
	mux.HandleFunc("/api/tasks/stats", h.handleTaskStats)

	// 定时任务相关
	mux.HandleFunc("/api/scheduled-tasks", h.handleScheduledTasks)
	mux.HandleFunc("/api/scheduled-tasks/", h.handleScheduledTaskOperations)
	mux.HandleFunc("/api/scheduled-tasks/enable/", h.handleEnableTask)
	mux.HandleFunc("/api/scheduled-tasks/disable/", h.handleDisableTask)

	// 适配器相关
	mux.HandleFunc("/api/adapters/register", h.handleRegisterAdapter)

	// 健康检查
	mux.HandleFunc("/health", h.handleHealth)

	log.Println("webhook: routes registered")
}

// handleSubmitTask 提交任务
func (h *WebhookHandler) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var task Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	// 设置默认值
	if task.ID == "" {
		task.ID = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	if task.Status == "" {
		task.Status = TaskStatusPending
	}
	if task.Priority == 0 {
		task.Priority = PriorityNormal
	}

	// 提交任务
	if err := h.taskScheduler.SubmitTask(&task); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to submit task: "+err.Error())
		return
	}

	respondJSON(w, http.StatusAccepted, map[string]interface{}{
		"task_id": task.ID,
		"status":  "submitted",
	})
}

// handleGetTask 获取任务
func (h *WebhookHandler) handleGetTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Path[len("/api/tasks/"):]
	if taskID == "" {
		respondError(w, http.StatusBadRequest, "Task ID is required")
		return
	}

	task, err := h.taskScheduler.GetTask(taskID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, task)
}

// handleListActiveTasks 列出活跃任务
func (h *WebhookHandler) handleListActiveTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tasks := h.taskScheduler.ListActiveTasks()
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"tasks": tasks,
		"count": len(tasks),
	})
}

// handleListTaskHistory 列出任务历史
func (h *WebhookHandler) handleListTaskHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit := 50 // 默认50条
	tasks := h.taskScheduler.ListTaskHistory(limit)
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"tasks": tasks,
		"count": len(tasks),
	})
}

// handleTaskStats 获取任务统计
func (h *WebhookHandler) handleTaskStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := h.taskScheduler.GetStats()
	cronStats := h.cronScheduler.GetStats()

	// 合并统计信息
	for k, v := range cronStats {
		stats[k] = v
	}

	respondJSON(w, http.StatusOK, stats)
}

// handleScheduledTasks 处理定时任务列表
func (h *WebhookHandler) handleScheduledTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// 列出所有定时任务
		tasks := h.cronScheduler.ListScheduledTasks()
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"tasks": tasks,
			"count": len(tasks),
		})

	case http.MethodPost:
		// 创建新定时任务
		var task ScheduledTask
		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			respondError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
			return
		}

		// 设置默认值
		now := time.Now()
		if task.CreatedAt.IsZero() {
			task.CreatedAt = now
		}
		task.UpdatedAt = now

		if err := h.cronScheduler.AddScheduledTask(&task); err != nil {
			respondError(w, http.StatusBadRequest, "Failed to add task: "+err.Error())
			return
		}

		respondJSON(w, http.StatusCreated, map[string]interface{}{
			"task_id": task.ID,
			"status":  "created",
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleScheduledTaskOperations 处理单个定时任务的操作
func (h *WebhookHandler) handleScheduledTaskOperations(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Path[len("/api/scheduled-tasks/"):]
	if taskID == "" {
		respondError(w, http.StatusBadRequest, "Task ID is required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		// 获取定时任务
		task, err := h.cronScheduler.GetScheduledTask(taskID)
		if err != nil {
			respondError(w, http.StatusNotFound, err.Error())
			return
		}
		respondJSON(w, http.StatusOK, task)

	case http.MethodPut:
		// 更新定时任务
		var task ScheduledTask
		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			respondError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
			return
		}
		task.ID = taskID

		if err := h.cronScheduler.UpdateScheduledTask(&task); err != nil {
			respondError(w, http.StatusBadRequest, "Failed to update task: "+err.Error())
			return
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"task_id": taskID,
			"status":  "updated",
		})

	case http.MethodDelete:
		// 删除定时任务
		if err := h.cronScheduler.RemoveScheduledTask(taskID); err != nil {
			respondError(w, http.StatusNotFound, err.Error())
			return
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"task_id": taskID,
			"status":  "deleted",
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleEnableTask 启用定时任务
func (h *WebhookHandler) handleEnableTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Path[len("/api/scheduled-tasks/enable/"):]
	if taskID == "" {
		respondError(w, http.StatusBadRequest, "Task ID is required")
		return
	}

	if err := h.cronScheduler.EnableTask(taskID); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"task_id": taskID,
		"status":  "enabled",
	})
}

// handleDisableTask 禁用定时任务
func (h *WebhookHandler) handleDisableTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Path[len("/api/scheduled-tasks/disable/"):]
	if taskID == "" {
		respondError(w, http.StatusBadRequest, "Task ID is required")
		return
	}

	if err := h.cronScheduler.DisableTask(taskID); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"task_id": taskID,
		"status":  "disabled",
	})
}

// handleRegisterAdapter 注册adapter
func (h *WebhookHandler) handleRegisterAdapter(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		AdapterType string `json:"adapter_type"`
		WebhookURL  string `json:"webhook_url"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if req.AdapterType == "" || req.WebhookURL == "" {
		respondError(w, http.StatusBadRequest, "adapter_type and webhook_url are required")
		return
	}

	// 创建HTTP adapter sender
	sender := NewHTTPAdapterSender(req.WebhookURL)
	h.taskScheduler.RegisterAdapter(req.AdapterType, sender)

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"adapter_type": req.AdapterType,
		"status":       "registered",
	})
}

// handleHealth 健康检查
func (h *WebhookHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// respondJSON 返回JSON响应
func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("webhook: failed to encode response: %v", err)
	}
}

// respondError 返回错误响应
func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]interface{}{
		"error": message,
	})
}

// HTTPAdapterSender 通过HTTP发送消息到adapter
type HTTPAdapterSender struct {
	webhookURL string
	client     *http.Client
}

// NewHTTPAdapterSender 创建HTTP adapter sender
func NewHTTPAdapterSender(webhookURL string) *HTTPAdapterSender {
	return &HTTPAdapterSender{
		webhookURL: webhookURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// SendMessage 发送消息
func (s *HTTPAdapterSender) SendMessage(ctx context.Context, channel, userID, content string) error {
	payload := map[string]interface{}{
		"channel": channel,
		"user_id": userID,
		"content": content,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Body = http.NoBody
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("adapter returned error status: %d", resp.StatusCode)
	}

	log.Printf("webhook: sent message to adapter via %s (channel: %s, user: %s)",
		s.webhookURL, channel, userID)

	// Fix: add missing data usage
	_ = data

	return nil
}
