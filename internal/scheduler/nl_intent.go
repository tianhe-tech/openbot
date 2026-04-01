package scheduler

import "strings"

// NLScheduleAction describes the scheduling action parsed from natural language.
type NLScheduleAction string

const (
	NLScheduleActionUnknown NLScheduleAction = "unknown"
	NLScheduleActionCreate  NLScheduleAction = "create"
	NLScheduleActionList    NLScheduleAction = "list"
	NLScheduleActionInfo    NLScheduleAction = "info"
	NLScheduleActionEnable  NLScheduleAction = "enable"
	NLScheduleActionDisable NLScheduleAction = "disable"
	NLScheduleActionDelete  NLScheduleAction = "delete"
	NLScheduleActionUpdate  NLScheduleAction = "update"
	NLScheduleActionRunOnce NLScheduleAction = "run_once"
)

// NLScheduleTaskSelector identifies the target task for management operations.
type NLScheduleTaskSelector struct {
	TaskID       string `json:"task_id"`
	NameContains string `json:"name_contains"`
}

// NLScheduleIntent is the structured representation of a scheduling request.
type NLScheduleIntent struct {
	Action          NLScheduleAction       `json:"action"`
	OriginalText    string                 `json:"original_text"`
	CronExpr        string                 `json:"cron_expr"`
	TimeExpression  string                 `json:"time_expression"`
	Timezone        string                 `json:"timezone"`
	Name            string                 `json:"name"`
	Content         string                 `json:"content"`
	Agent           string                 `json:"agent"`
	Selector        NLScheduleTaskSelector `json:"selector"`
	Ambiguities     []string               `json:"ambiguities"`
	Confidence      float64                `json:"confidence"`
	RequiresConfirm bool                   `json:"requires_confirm"`
	ConfirmMessage  string                 `json:"confirm_message"`
}

// IsWriteAction returns true when the action mutates scheduler state.
func (i *NLScheduleIntent) IsWriteAction() bool {
	switch i.Action {
	case NLScheduleActionCreate, NLScheduleActionEnable, NLScheduleActionDisable,
		NLScheduleActionDelete, NLScheduleActionUpdate, NLScheduleActionRunOnce:
		return true
	default:
		return false
	}
}

// NeedsClarification returns true when parser found ambiguities.
func (i *NLScheduleIntent) NeedsClarification() bool {
	return len(i.Ambiguities) > 0
}

// Normalize trims string fields to keep parser and service behavior stable.
func (i *NLScheduleIntent) Normalize() {
	i.OriginalText = strings.TrimSpace(i.OriginalText)
	i.CronExpr = strings.TrimSpace(i.CronExpr)
	i.TimeExpression = strings.TrimSpace(i.TimeExpression)
	i.Timezone = strings.TrimSpace(i.Timezone)
	i.Name = strings.TrimSpace(i.Name)
	i.Content = strings.TrimSpace(i.Content)
	i.Agent = strings.TrimSpace(i.Agent)
	i.Selector.TaskID = strings.TrimSpace(i.Selector.TaskID)
	i.Selector.NameContains = strings.TrimSpace(i.Selector.NameContains)
}
