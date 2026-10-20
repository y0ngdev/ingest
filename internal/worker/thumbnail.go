package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/gif"  // register GIF decoder
	"image/jpeg"
	_ "image/png"  // register PNG decoder
	"log/slog"
	"time"

	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp" // register WebP decoder

	"ingest/internal/events"
	"ingest/internal/lib"
	"ingest/internal/storage"
)

const (
	thumbMaxW = 256
	thumbMaxH = 256
	thumbKey  = "_thumb" // appended to the original key
)

// Thumbnailer subscribes to UploadComplete events and writes a 256×256
// JPEG thumbnail for every image upload (JPEG, PNG, GIF, WebP).
// Non-image files (PDF, MP4) are silently skipped.
// Each generation is retried up to 3 times with exponential backoff.
type Thumbnailer struct {
	backend storage.Backend
}

func NewThumbnailer(backend storage.Backend) *Thumbnailer {
	return &Thumbnailer{backend: backend}
}

// Register subscribes the thumbnailer to upload-complete events on q.
func (t *Thumbnailer) Register(q events.Consumer) {
	q.Subscribe(func(ctx context.Context, e events.Event) error {
		ev, ok := e.(events.UploadComplete)
		if !ok {
			return nil
		}
		err := lib.Retry(ctx, 3, time.Second, func() error {
			return t.generate(ctx, ev)
		})
		if err != nil {
			slog.Error("thumbnail: all retries exhausted", "key", ev.Key, "err", err)
		}
		// Thumbnail failure is non-fatal — the original upload is already stored.
		return nil
	})
}

func (t *Thumbnailer) generate(ctx context.Context, ev events.UploadComplete) error {
	r, err := t.backend.Retrieve(ctx, ev.Key)
	if err != nil {
		return fmt.Errorf("thumbnail: retrieve %q: %w", ev.Key, err)
	}
	defer r.Close()

	// image.Decode dispatches to whichever decoder was registered for this
	// file's magic bytes (JPEG, PNG, GIF, WebP via the blank imports above).
	src, format, err := image.Decode(r)
	if err != nil {
		if errors.Is(err, image.ErrFormat) {
			// Not an image (PDF, MP4, unknown) — skip without retrying.
			slog.Debug("thumbnail: skipping non-image", "key", ev.Key)
			return nil
		}
		return fmt.Errorf("thumbnail: decode %q: %w", ev.Key, err)
	}

	// Scale into a new RGBA canvas that fits within 256×256, maintaining
	// aspect ratio and never upscaling.
	dstRect := fitRect(src.Bounds(), thumbMaxW, thumbMaxH)
	dst := image.NewRGBA(dstRect)

	// draw.BiLinear produces smooth results without the blocky artefacts of
	// nearest-neighbour; it's the right default for photo content.
	draw.BiLinear.Scale(dst, dstRect, src, src.Bounds(), draw.Over, nil)

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 85}); err != nil {
		return fmt.Errorf("thumbnail: encode: %w", err)
	}

	thumbKey := ev.Key + thumbKey
	if err := t.backend.Store(ctx, thumbKey, &buf); err != nil {
		return fmt.Errorf("thumbnail: store %q: %w", thumbKey, err)
	}

	slog.Info("thumbnail: generated", "key", ev.Key, "format", format,
		"src_w", src.Bounds().Dx(), "src_h", src.Bounds().Dy(),
		"thumb_w", dstRect.Dx(), "thumb_h", dstRect.Dy(),
		"bytes", buf.Len())
	return nil
}

// fitRect returns the largest rectangle that fits within maxW×maxH while
// maintaining src's aspect ratio. Never upscales.
func fitRect(src image.Rectangle, maxW, maxH int) image.Rectangle {
	w, h := src.Dx(), src.Dy()
	if w == 0 || h == 0 || (w <= maxW && h <= maxH) {
		return image.Rect(0, 0, w, h)
	}
	scale := min(float64(maxW)/float64(w), float64(maxH)/float64(h))
	return image.Rect(0, 0, int(float64(w)*scale), int(float64(h)*scale))
}
