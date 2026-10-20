package events

import (
	"context"
	"sync"
)

// MemoryQueue is an in-process, channel-backed queue.
// Suitable for development and single-process deployments.
// Swap it for a Redis Streams or Kafka implementation without changing any
// subscriber code — both implement Publisher and Consumer.
type MemoryQueue struct {
	ch       chan Event
	mu       sync.RWMutex
	handlers []Handler
	wg       sync.WaitGroup
}

func NewMemoryQueue(bufSize int) *MemoryQueue {
	return &MemoryQueue{ch: make(chan Event, bufSize)}
}

// Publish sends e to the queue buffer. Blocks only if the buffer is full.
func (q *MemoryQueue) Publish(_ context.Context, e Event) error {
	q.ch <- e
	return nil
}

// Subscribe registers h to be called for every event.
// Must be called before Run.
func (q *MemoryQueue) Subscribe(h Handler) {
	q.mu.Lock()
	q.handlers = append(q.handlers, h)
	q.mu.Unlock()
}

// Run drains the queue, dispatching each event to all registered handlers
// in separate goroutines. The WaitGroup tracks in-flight calls so a graceful
// shutdown (ctx cancel) waits for every handler to finish before returning.
func (q *MemoryQueue) Run(ctx context.Context) {
	for {
		select {
		case e := <-q.ch:
			q.mu.RLock()
			hs := make([]Handler, len(q.handlers))
			copy(hs, q.handlers)
			q.mu.RUnlock()

			for _, h := range hs {
				h := h
				q.wg.Add(1)
				go func() {
					defer q.wg.Done()
					_ = h(ctx, e)
				}()
			}
		case <-ctx.Done():
			q.wg.Wait()
			return
		}
	}
}
