package skillgen

import (
	"context"
	"fmt"
	"time"

	"github.com/user/opencode-gateway/internal/adapters/base"
)

// RegistryNotifier implements Notifier over a base.AdapterRegistry.
// When a candidate is ready it sends a short message to the originating user
// via whichever platform adapter the event came from.
type RegistryNotifier struct {
	Registry *base.AdapterRegistry
}

// NewRegistryNotifier constructs a notifier.
func NewRegistryNotifier(registry *base.AdapterRegistry) *RegistryNotifier {
	return &RegistryNotifier{Registry: registry}
}

// NotifyCandidate implements Notifier.
func (n *RegistryNotifier) NotifyCandidate(adapter, userID, candidateID, title string, approvalRequired bool) error {
	var msg string
	if approvalRequired {
		msg = fmt.Sprintf("💡 我从刚才的会话里识别出一个可复用的技能【%s】\n候选 ID：%s\n发送 /skill-view %s 查看，/skill-approve %s 批准，/skill-reject %s 拒绝。",
			title, candidateID, candidateID, candidateID, candidateID)
	} else {
		msg = fmt.Sprintf("✅ 已自动生成并安装技能【%s】（候选 ID：%s）。发送 /skill-view %s 查看详情。",
			title, candidateID, candidateID)
	}
	return n.send(adapter, userID, msg)
}

func (n *RegistryNotifier) NotifyFailure(adapter, userID, stage, reason string) error {
	return n.send(adapter, userID, fmt.Sprintf("⚠️ 技能自动生成未完成（阶段：%s）\n%s", stage, reason))
}

func (n *RegistryNotifier) send(adapter, userID, msg string) error {
	if n == nil || n.Registry == nil || adapter == "" || userID == "" {
		return fmt.Errorf("skillgen: notifier requires a registry and recipient")
	}
	a, ok := n.Registry.Get(adapter)
	if !ok {
		return fmt.Errorf("skillgen: no adapter registered for %s", adapter)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return a.SendToUser(ctx, userID, msg)
}
