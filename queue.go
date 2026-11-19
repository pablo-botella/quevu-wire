package qwire

import "sync"

// queue is an unbounded mailbox: putting never blocks, which is what the
// goroutine reading the connection needs so one slow listener cannot hold
// up everybody else.
type queue[T any] struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []T
	closed bool
}

func newQueue[T any]() *queue[T] {
	q := &queue[T]{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *queue[T]) put(v T) {
	q.mu.Lock()
	if !q.closed {
		q.items = append(q.items, v)
		q.cond.Signal()
	}
	q.mu.Unlock()
}

// get waits for the next item. ok is false once the queue is closed and
// empty.
func (q *queue[T]) get() (v T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return v, false
	}
	v = q.items[0]
	var zero T
	q.items[0] = zero
	q.items = q.items[1:]
	return v, true
}

func (q *queue[T]) close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}
