package wechat

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestOutboundStartupClassifiesBeforeDispatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "outbound.db")
	var sent []string
	send := func(item *queuedOutboundText) error {
		sent = append(sent, item.Content)
		return nil
	}
	q, err := newOutboundTextQueue(path, time.Second, 1600, send)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		event    string
		body     string
		critical bool
	}{
		{"done", "obsolete completion", true},
		{"done", "obsolete noncritical completion", false},
		{"question", "obsolete question", true},
		{"final", "valid final", true},
		{"error_partial", "valid partial", true},
		{"final", "old final", true},
		{"skippable", "expired progress", false},
	} {
		if err := q.EnqueueText("alice", "ses_original", "token", item.event, item.body, item.critical); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET created_at = ?
		WHERE content = 'old final'`, time.Now().Add(-48*time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET expires_at = 1
		WHERE content = 'expired progress' OR (event_type = 'done' AND critical = 0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(`ALTER TABLE wechat_outbound_queue DROP COLUMN recovery_until`); err != nil {
		t.Fatal(err)
	}
	q.Stop()
	q, err = newOutboundTextQueue(path, time.Second, 1600, send)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Stop()
	q.setMinGap(0)
	stats, err := q.snapshotStats()
	if err != nil || stats.Pending != 2 || stats.Parked != 4 || stats.Due != 2 {
		t.Fatalf("wrong startup classification: %+v err=%v", stats, err)
	}
	for i := 0; i < 2; i++ {
		if did, err := q.dispatchOne(); err != nil || !did {
			t.Fatalf("dispatch failed did=%v err=%v", did, err)
		}
	}
	if len(sent) != 2 || sent[0] != "valid final" || sent[1] != "valid partial" {
		t.Fatalf("unexpected restart deliveries: %v", sent)
	}
}

func TestOutboundRecoveryPreservesEvidenceAndSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbound.db")
	var delivered *queuedOutboundText
	send := func(item *queuedOutboundText) error { copy := *item; delivered = &copy; return nil }
	q, err := newOutboundTextQueue(path, time.Second, 1600, send)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []string{"final", "done", "question", "todo"} {
		if err := q.EnqueueText("alice", "ses_original", "token", event, event, true); err != nil {
			t.Fatal(err)
		}
	}
	created := time.Now().Add(-48 * time.Hour).UnixMilli()
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET parked = 1, created_at = ?, attempts = 4`, created); err != nil {
		t.Fatal(err)
	}
	var clientID string
	if err := q.db.QueryRow(`SELECT client_id FROM wechat_outbound_queue WHERE event_type = 'final'`).Scan(&clientID); err != nil {
		t.Fatal(err)
	}
	beforeRecovery := time.Now().UnixMilli()
	result, err := q.RecoverParked("alice", "ses_")
	afterRecovery := time.Now().UnixMilli()
	if err != nil || result.Recovered != 1 || result.SkippedDone != 1 || result.SkippedPrompts != 1 || result.SkippedOther != 1 {
		t.Fatalf("wrong explicit recovery: %+v err=%v", result, err)
	}
	q.Stop()
	q, err = newOutboundTextQueue(path, time.Second, 1600, send)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Stop()
	q.setMinGap(0)
	if did, err := q.dispatchOne(); err != nil || !did {
		t.Fatalf("recovered old body did not dispatch: did=%v err=%v", did, err)
	}
	if delivered == nil || delivered.CreatedAt != created || delivered.ClientID != clientID || delivered.Attempts != 0 ||
		delivered.RecoveryUntil < beforeRecovery+maxQueuedItemAge.Milliseconds() ||
		delivered.RecoveryUntil > afterRecovery+maxQueuedItemAge.Milliseconds() {
		t.Fatalf("original evidence or recovery window lost: %+v", delivered)
	}
	if did, err := q.dispatchOne(); err != nil || did {
		t.Fatalf("excluded notices were resurrected: did=%v err=%v", did, err)
	}
}

func TestOutboundRecoveryWindowExpiresAndFilterIsLiteral(t *testing.T) {
	var sends int
	q, err := newOutboundTextQueue(filepath.Join(t.TempDir(), "outbound.db"), time.Second, 1600,
		func(*queuedOutboundText) error { sends++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer q.Stop()
	for _, session := range []string{"ses_%", "ses_other"} {
		if err := q.EnqueueText("alice", session, "", "final", session, true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET parked = 1`); err != nil {
		t.Fatal(err)
	}
	result, err := q.RecoverParked("alice", "ses_%")
	if err != nil || result.Recovered != 1 {
		t.Fatalf("session prefix was interpreted as SQL wildcards: %+v err=%v", result, err)
	}
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET recovery_until = 1 WHERE parked = 0`); err != nil {
		t.Fatal(err)
	}
	if did, err := q.dispatchOne(); err != nil || !did || sends != 0 {
		t.Fatalf("expired recovery window sent content: did=%v sends=%d err=%v", did, sends, err)
	}
	stats, err := q.snapshotStats()
	if err != nil || stats.Parked != 2 || stats.Pending != 0 {
		t.Fatalf("expired recovery did not repark: %+v err=%v", stats, err)
	}
}

func TestOutboundFairnessCannotBypassExpiry(t *testing.T) {
	var sends int
	q, err := newOutboundTextQueue(filepath.Join(t.TempDir(), "outbound.db"), time.Second, 1600,
		func(*queuedOutboundText) error { sends++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer q.Stop()
	if err := q.EnqueueText("alice", "ses_original", "", "question", "current prompt", true); err != nil {
		t.Fatal(err)
	}
	if err := q.EnqueueText("alice", "ses_original", "", "final", "expired content", true); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET created_at = 1 WHERE event_type = 'final'`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxHighStreakPerUser; i++ {
		q.bumpHighStreak("alice")
	}
	if did, err := q.dispatchOne(); err != nil || !did || sends != 0 {
		t.Fatalf("fairness sent expired content: did=%v sends=%d err=%v", did, sends, err)
	}
}

func TestOutboundStatsSeparateParkedWaitingAndFailures(t *testing.T) {
	q, err := newOutboundTextQueue(filepath.Join(t.TempDir(), "outbound.db"), time.Second, 1600,
		func(*queuedOutboundText) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"due", "waiting", "parked"} {
		if err := q.EnqueueText("alice", "ses_original", "", "final", body, true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET next_attempt_at = ?, attempts = 1 WHERE content = 'waiting'`,
		time.Now().Add(time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET parked = 1 WHERE content = 'parked'`); err != nil {
		t.Fatal(err)
	}
	stats, err := q.snapshotStats()
	if err != nil || stats.Pending != 2 || stats.Parked != 1 || stats.Waiting != 1 || stats.Due != 1 || stats.Failed != 1 {
		t.Fatalf("misleading queue stats: %+v err=%v", stats, err)
	}
	if count, err := q.PendingForUser("alice"); err != nil || count != 3 {
		t.Fatalf("legacy inbound count contract changed: count=%d err=%v", count, err)
	}
	q.Stop()
	if _, err := q.snapshotStats(); err == nil {
		t.Fatal("closed database produced success-shaped empty statistics")
	}
}

func TestOutboundStartupClassificationFailureIsExplicit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbound.db")
	q, err := newOutboundTextQueue(path, time.Second, 1600, func(*queuedOutboundText) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := q.EnqueueText("alice", "ses_original", "", "done", "old completion", true); err != nil {
		t.Fatal(err)
	}
	if err := q.EnqueueText("alice", "ses_original", "", "skippable", "expired progress", false); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET expires_at = 1 WHERE event_type = 'skippable'`); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec(`CREATE TRIGGER reject_startup BEFORE DELETE ON wechat_outbound_queue
		BEGIN SELECT RAISE(FAIL, 'classification failure'); END`); err != nil {
		t.Fatal(err)
	}
	q.Stop()
	reopened, err := newOutboundTextQueue(path, time.Second, 1600, func(*queuedOutboundText) error {
		t.Error("unsafe startup dispatched an old notice")
		return nil
	})
	if err == nil || reopened != nil {
		t.Fatalf("classification failure ignored: queue=%v err=%v", reopened, err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var active int
	if err := db.QueryRow(`SELECT COUNT(*) FROM wechat_outbound_queue WHERE parked = 0`).Scan(&active); err != nil || active != 2 {
		t.Fatalf("failed startup partially committed recovery: active=%d err=%v", active, err)
	}
	handler := &Handler{cfg: Config{BaseURL: "http://unused"}, outboundInitErr: errors.New("classification failure")}
	if err := handler.Start(context.Background()); err == nil {
		t.Fatal("handler started after outbound recovery failure")
	}
}
