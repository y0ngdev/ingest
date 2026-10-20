package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	clamd "github.com/dutchcoders/go-clamd"

	"ingest/internal/events"
	"ingest/internal/lib"
	"ingest/internal/metrics"
	"ingest/internal/services"
	"ingest/internal/storage"
)

// Scanner subscribes to UploadComplete events and checks each file with ClamAV.
//
// Status flow: complete → scanning → clean | quarantined | error
//
// Quarantined files are NOT deleted — the key remains in storage for audit.
// If all retry attempts fail the session is marked "error" so the client can
// query GET /uploads/{id} and see that something went wrong.
type Scanner struct {
	clamd    *clamd.Clamd
	sessions *services.Store
	backend  storage.Backend
}

// NewScanner connects to ClamAV at socketAddr.
// socketAddr examples:
//
//	"unix:///var/run/clamav/clamd.ctl"   (Unix socket, typical on Linux)
//	"tcp://localhost:3310"               (TCP, useful in Docker)
func NewScanner(socketAddr string, sessions *services.Store, backend storage.Backend) *Scanner {
	return &Scanner{
		clamd:    clamd.NewClamd(socketAddr),
		sessions: sessions,
		backend:  backend,
	}
}

// Register subscribes the scanner to upload-complete events on q.
// Each event is retried up to 3 times with exponential backoff before being
// marked as error.
func (sc *Scanner) Register(q events.Consumer) {
	q.Subscribe(func(ctx context.Context, e events.Event) error {
		ev, ok := e.(events.UploadComplete)
		if !ok {
			return nil
		}

		err := lib.Retry(ctx, 3, 2*time.Second, func() error {
			return sc.scan(ctx, ev)
		})
		if err != nil {
			slog.Error("scan: all retries exhausted", "key", ev.Key, "err", err)
			if sess, serr := sc.sessions.Get(ev.ID); serr == nil {
				sess.SetScanStatus(services.ScanStatusError)
			}
		}
		return err
	})
}

func (sc *Scanner) scan(ctx context.Context, ev events.UploadComplete) error {
	sess, err := sc.sessions.Get(ev.ID)
	if err != nil {
		// Session may be gone after a server restart — not retryable.
		return fmt.Errorf("scan: session %q: %w", ev.ID, err)
	}

	sess.SetScanStatus(services.ScanStatusScanning)

	r, err := sc.backend.Retrieve(ctx, ev.Key)
	if err != nil {
		return fmt.Errorf("scan: retrieve %q: %w", ev.Key, err)
	}
	defer r.Close()

	abort := make(chan bool)
	results, err := sc.clamd.ScanStream(r, abort)
	if err != nil {
		return fmt.Errorf("scan: clamd stream: %w", err)
	}

	for res := range results {
		switch res.Status {
		case clamd.RES_FOUND:
			slog.Warn("scan: virus found", "key", ev.Key, "threat", res.Description)
			sess.SetScanStatus(services.ScanStatusQuarantined)
			metrics.UploadsFlagged.Add(1)
			return nil
		case clamd.RES_ERROR:
			return fmt.Errorf("scan: clamd reported error for %q: %s", ev.Key, res.Description)
		}
	}

	sess.SetScanStatus(services.ScanStatusClean)
	slog.Info("scan: clean", "key", ev.Key)
	return nil
}
