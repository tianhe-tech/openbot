package opencode

import (
	"context"
	"log"
	"sync"
)

type QueuedMessage struct {
	Ctx        context.Context
	Payload    MessagePayload
	Callback   StreamCallback
	EventCb    StreamEventCallback
	ResultChan chan *QueueResult
}

type QueueResult struct {
	Response Response
	Error    error
}

type MessageQueue struct {
	items    chan *QueuedMessage
	workers  int
	handler  func(ctx context.Context, payload MessagePayload, cb StreamCallback, eventCb StreamEventCallback) (Response, error)
	wg       sync.WaitGroup
	stopChan chan struct{}
}

func NewMessageQueue(handler func(ctx context.Context, payload MessagePayload, cb StreamCallback, eventCb StreamEventCallback) (Response, error), workers int) *MessageQueue {
	if workers <= 0 {
		workers = 4
	}
	q := &MessageQueue{
		items:    make(chan *QueuedMessage, 256),
		workers:  workers,
		handler:  handler,
		stopChan: make(chan struct{}),
	}
	q.start()
	return q
}

func (q *MessageQueue) start() {
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	log.Printf("opencode: message queue started with %d workers", q.workers)
}

func (q *MessageQueue) worker() {
	defer q.wg.Done()
	for {
		select {
		case <-q.stopChan:
			return
		case msg := <-q.items:
			resp, err := q.handler(msg.Ctx, msg.Payload, msg.Callback, msg.EventCb)
			msg.ResultChan <- &QueueResult{Response: resp, Error: err}
		}
	}
}

func (q *MessageQueue) Enqueue(ctx context.Context, payload MessagePayload, cb StreamCallback, eventCb StreamEventCallback) *QueueResult {
	resultChan := make(chan *QueueResult, 1)
	msg := &QueuedMessage{
		Ctx:        ctx,
		Payload:    payload,
		Callback:   cb,
		EventCb:    eventCb,
		ResultChan: resultChan,
	}

	select {
	case q.items <- msg:
		select {
		case result := <-resultChan:
			return result
		case <-ctx.Done():
			return &QueueResult{Error: ctx.Err()}
		}
	case <-ctx.Done():
		return &QueueResult{Error: ctx.Err()}
	default:
		go func() {
			q.items <- msg
		}()
		select {
		case result := <-resultChan:
			return result
		case <-ctx.Done():
			return &QueueResult{Error: ctx.Err()}
		}
	}
}

func (q *MessageQueue) EnqueueAsync(ctx context.Context, payload MessagePayload, cb StreamCallback, eventCb StreamEventCallback, callback func(resp Response, err error)) {
	msg := &QueuedMessage{
		Ctx:        ctx,
		Payload:    payload,
		Callback:   cb,
		EventCb:    eventCb,
		ResultChan: make(chan *QueueResult, 1),
	}

	go func() {
		select {
		case q.items <- msg:
			select {
			case result := <-msg.ResultChan:
				if callback != nil {
					callback(result.Response, result.Error)
				}
			case <-ctx.Done():
				if callback != nil {
					callback(Response{}, ctx.Err())
				}
			}
		default:
			resp, err := q.handler(ctx, payload, cb, eventCb)
			if callback != nil {
				callback(resp, err)
			}
		}
	}()
}

func (q *MessageQueue) Stop() {
	close(q.stopChan)
	q.wg.Wait()
	close(q.items)
	log.Printf("opencode: message queue stopped")
}

func (q *MessageQueue) Len() int {
	return len(q.items)
}
