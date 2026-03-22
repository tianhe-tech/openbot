package opencode

// StreamEventKind defines structured streaming event categories.
type StreamEventKind string

const (
	StreamEventSessionReady    StreamEventKind = "session_ready"
	StreamEventTextDelta       StreamEventKind = "text_delta"
	StreamEventThinking        StreamEventKind = "thinking"
	StreamEventTool            StreamEventKind = "tool"
	StreamEventStep            StreamEventKind = "step"
	StreamEventTodo            StreamEventKind = "todo"
	StreamEventTodoSnapshot    StreamEventKind = "todo_snapshot"
	StreamEventDiffSnapshot    StreamEventKind = "diff_snapshot"
	StreamEventDiffSummary     StreamEventKind = "diff_summary"
	StreamEventQuestionAsked   StreamEventKind = "question_asked"
	StreamEventPermissionAsked StreamEventKind = "permission_asked"
	StreamEventInfo            StreamEventKind = "info"
	StreamEventError           StreamEventKind = "error"
	StreamEventFlush           StreamEventKind = "flush"
)

// StreamEvent represents a structured streaming event.
type StreamEvent struct {
	Kind      StreamEventKind `json:"kind"`
	SessionID string          `json:"session_id,omitempty"`
	Content   string          `json:"content,omitempty"`
	Question  *Question       `json:"question,omitempty"`
	Todos     []TodoItem      `json:"todos,omitempty"`
	Diff      []FileDiff      `json:"diff,omitempty"`
	RawType   string          `json:"raw_type,omitempty"`
}

// StreamEventCallback handles structured streaming events.
type StreamEventCallback func(event StreamEvent) error
