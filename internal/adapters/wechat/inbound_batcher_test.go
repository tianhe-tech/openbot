package wechat

import (
	"context"
	"testing"
	"time"
)

func TestInboundBatchPreservesDeliveryIdentities(t *testing.T) {
	var delivered *WeixinMessage
	batcher := newInboundBatcher(time.Hour, time.Hour, 1000, func(_ context.Context, message *WeixinMessage) error { delivered = message; return nil })
	for index, text := range []string{"first", "second"} {
		message := &WeixinMessage{MessageID: int64(index + 1), FromUserID: "owner"}
		setMessageText(message, text)
		batcher.enqueueText(context.Background(), message, text)
	}
	batcher.flushAll(context.Background())
	if delivered == nil || firstTextBody(delivered) != "first\nsecond" || len(delivered.deliveryIDs) != 2 {
		t.Fatalf("batch identity lost: %+v", delivered)
	}
	input := wechatCaptureInput{DeliveryIDs: delivered.deliveryIDs}
	if input.deliveryID() != (wechatCaptureInput{DeliveryIDs: []string{"message:1", "message:2"}}).deliveryID() {
		t.Fatal("batch identity unstable")
	}
	if input.deliveryID() == (wechatCaptureInput{DeliveryIDs: []string{"message:1"}}).deliveryID() {
		t.Fatal("batch collapsed into its first delivery")
	}
}

func TestInboundSyntheticIdentityIsCarriedNotRegenerated(t *testing.T) {
	message := &WeixinMessage{}
	ensureDeliveryIDs(message)
	first := message.deliveryIDs[0]
	ensureDeliveryIDs(message)
	if message.deliveryIDs[0] != first {
		t.Fatal("synthetic identity regenerated")
	}
	input := captureInputForDispatch("original", nil)
	carried := captureInputForDispatch("wrapped", []wechatCaptureInput{input})
	if carried.deliveryID() != input.deliveryID() || carried.OriginalContent != "original" {
		t.Fatal("dispatch replaced carried identity or input")
	}
}
