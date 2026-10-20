package worker

import (
	"context"
	"log/slog"

	"ingest/internal/events"
)

// RegisterThumbnail subscribes to UploadComplete events and generates
// thumbnails. This is a stub — real implementation would decode the image
// with image.Decode, resize with golang.org/x/image/draw, and write the
// result back to storage.
func RegisterThumbnail(q events.Consumer) {
	q.Subscribe(func(ctx context.Context, e events.Event) error {
		ev, ok := e.(events.UploadComplete)
		if !ok {
			return nil
		}
		slog.Info("thumbnail: processing", "key", ev.Key, "size", ev.Size)
		// TODO: decode → resize → store thumbnail
		return nil
	})
}
