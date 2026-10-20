package validate

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// DefaultAllowed is the default set of accepted MIME types.
// Override per-handler by assigning a different map.
var DefaultAllowed = map[string]bool{
	"image/jpeg":      true,
	"image/png":       true,
	"image/gif":       true,
	"image/webp":      true,
	"application/pdf": true,
	"video/mp4":       true,
}

// MaxSize is the default per-upload ceiling (512 MiB).
const MaxSize int64 = 512 << 20

// PeekMIME reads up to 512 bytes from r to detect the actual content type via
// magic bytes (not the client-supplied Content-Type header), then returns:
//   - the detected MIME string
//   - a reconstructed io.Reader that replays those bytes so the full stream
//     is still readable by the caller
//
// For broader format coverage beyond what net/http.DetectContentType supports,
// swap in github.com/h2non/filetype: kind, _ := filetype.Match(buf).
func PeekMIME(r io.Reader) (mime string, full io.Reader, err error) {
	buf := make([]byte, 512)
	n, err := io.ReadFull(r, buf)
	// ErrUnexpectedEOF just means the body is smaller than 512 bytes — valid.
	if err != nil && err != io.ErrUnexpectedEOF {
		return "", nil, fmt.Errorf("validate: peek: %w", err)
	}
	buf = buf[:n]
	mime = http.DetectContentType(buf)
	// MultiReader replays the peeked bytes before the remaining body so
	// nothing is lost from the stream.
	return mime, io.MultiReader(bytes.NewReader(buf), r), nil
}

// CheckType returns an error if mime is not in allowed.
func CheckType(mime string, allowed map[string]bool) error {
	if !allowed[mime] {
		return fmt.Errorf("validate: type %q not allowed", mime)
	}
	return nil
}

// CheckSize returns an error if size exceeds max.
func CheckSize(size, max int64) error {
	if max > 0 && size > max {
		return fmt.Errorf("validate: file too large (%d B, max %d B)", size, max)
	}
	return nil
}
