package message

// Message is implemented by types that can be processed by a Queue.
type Message interface {
	// Type returns the type of this Message.
	Type() string
}

// Queue is implemented by types that can serve as message queues.
type Queue interface {
	// Cleanly shutdown the queue.
	Close() error

	// Enqueue inserts a message to the end of the queue.
	Enqueue(msg Message) error

	// PendingMessages returns true if the queue contains any messages.
	PendingMessages() bool

	// Flush drops all pending messages from the queue.
	DiscardMessages() error

	// Messages returns an iterator for accessing the queued messages.
	Messages() Iterator
}

// Iterator provides an API for iterating a list of messages.
type Iterator interface {
	// Next advances the iterator so that the next message can be retrieved
	// via a call to Message(). If no more messages are available or an
	// error occurs, Next() returns false.
	Next() bool

	// Message returns the message currently pointed to by the iterator.
	Message() Message

	// Error returns the last error that the iterator encountered.
	Error() error
}

// QueueFactory is a function that can create new Queue instances.
type QueueFactory func() Queue
