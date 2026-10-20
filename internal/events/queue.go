package events

import "context"

// Event is the base interface for all queue messages. Concrete types carry
// their own payload and declare their type string — no wrapper struct needed.
type Event interface {
	EventType() string
}

// Handler is the signature every consumer function must match.
type Handler func(ctx context.Context, e Event) error

// Publisher sends events to a queue.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

// Consumer receives events. Register handlers with Subscribe before calling Run.
type Consumer interface {
	Subscribe(h Handler)
	// Run dispatches events until ctx is cancelled, then waits for all
	// in-flight handler goroutines to finish before returning.
	Run(ctx context.Context)
}

// --- Event types ---

// UploadComplete is published when a session finishes and its checksum has
// been registered in the dedup index.
type UploadComplete struct {
	ID       string
	Key      string
	Checksum string
	Size     int64
}

func (UploadComplete) EventType() string { return "upload.complete" }
