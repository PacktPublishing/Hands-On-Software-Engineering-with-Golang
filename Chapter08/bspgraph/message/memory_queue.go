package message

import (
	"sync"
)

// inMemoryQueue implements a queue that stores messages in memory. Messages
// can be enqueued concurrently but the returned iterator is not safe for
// concurrent access.
type inMemoryQueue struct {
	mu   sync.Mutex
	msgs []Message

	latchedMsg Message
}

// NewInMemoryQueue creates a new in-memory queue instance. This function can
// serve as a QueueFactory.
func NewInMemoryQueue() Queue {
	return new(inMemoryQueue)
}

// Enqueue implements Queue.
func (q *inMemoryQueue) Enqueue(msg Message) error {
	q.mu.Lock()
	q.msgs = append(q.msgs, msg)
	q.mu.Unlock()
	return nil
}

// PendingMessages implements Queue.
func (q *inMemoryQueue) PendingMessages() bool {
	q.mu.Lock()
	pending := len(q.msgs) != 0
	q.mu.Unlock()
	return pending
}

// DiscardMessages implements Queue.
func (q *inMemoryQueue) DiscardMessages() error {
	q.mu.Lock()
	q.msgs = q.msgs[:0]
	q.latchedMsg = nil
	q.mu.Unlock()
	return nil
}

// Close implements Queue.
func (*inMemoryQueue) Close() error { return nil }

// Messages implements Queue.
func (q *inMemoryQueue) Messages() Iterator { return q }

// Next implements Iterator.
func (q *inMemoryQueue) Next() bool {
	q.mu.Lock()
	qLen := len(q.msgs)
	if qLen == 0 {
		q.mu.Unlock()
		return false
	}

	// Dequeue message from the tail of the queue.
	q.latchedMsg = q.msgs[qLen-1]
	q.msgs = q.msgs[:qLen-1]
	q.mu.Unlock()
	return true
}

// Message implements Iterator.
func (q *inMemoryQueue) Message() Message {
	q.mu.Lock()
	msg := q.latchedMsg
	q.mu.Unlock()
	return msg
}

// Error implements Iterator.
func (*inMemoryQueue) Error() error { return nil }
