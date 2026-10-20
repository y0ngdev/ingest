package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"ingest/internal/dedup"
	"ingest/internal/events"
	"ingest/internal/metrics"
	"ingest/internal/services"
	"ingest/internal/storage"
	"ingest/internal/validate"
)

// UploadHandler wires the upload endpoints to a Backend, session Store,
// dedup Index, event Publisher, and validation config.
type UploadHandler struct {
	storage      storage.Backend
	sessions     *services.Store
	dedup        *dedup.Index
	pub          events.Publisher
	allowedTypes map[string]bool
	maxSize      int64
}

func New(b storage.Backend, sessions *services.Store, idx *dedup.Index, pub events.Publisher) *UploadHandler {
	return &UploadHandler{
		storage:      b,
		sessions:     sessions,
		dedup:        idx,
		pub:          pub,
		allowedTypes: validate.DefaultAllowed,
		maxSize:      validate.MaxSize,
	}
}

type uploadResponse struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	ScanStatus   string `json:"scan_status,omitempty"`
	Size         int64  `json:"size,omitempty"`
	Deduplicated bool   `json:"deduplicated,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// CreateUpload handles POST /uploads.
// Validates Upload-Length against the configured max size before creating a session.
func (h *UploadHandler) CreateUpload(w http.ResponseWriter, r *http.Request) {
	var totalSize int64
	if s := r.Header.Get("Upload-Length"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid Upload-Length")
			return
		}
		if err := validate.CheckSize(n, h.maxSize); err != nil {
			writeError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		totalSize = n
	}
	sess := h.sessions.Create(totalSize)
	metrics.UploadsStarted.Add(1)
	w.Header().Set("Location", "/uploads/"+sess.ID)
	writeJSON(w, http.StatusCreated, uploadResponse{ID: sess.ID, Status: string(services.StatusPending)})
}

// AppendChunk handles PATCH /uploads/{id}.
//
// On the first chunk (offset == 0) it peeks the first 512 bytes to detect
// the actual MIME type via magic bytes — independent of whatever
// Content-Type the client sent — and rejects types not in the allowlist.
//
// On completion it finalizes storage, checks the dedup index, and publishes
// an UploadComplete event so async workers (thumbnail, scan) can proceed
// without blocking the HTTP response.
func (h *UploadHandler) AppendChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sess, err := h.sessions.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}

	offset, err := parseOffset(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Magic-byte validation on the first chunk only.
	body := io.Reader(r.Body)
	if offset == 0 {
		mime, full, err := validate.PeekMIME(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("peek failed: %v", err))
			return
		}
		if err := validate.CheckType(mime, h.allowedTypes); err != nil {
			writeError(w, http.StatusUnsupportedMediaType, err.Error())
			return
		}
		body = full
	}

	if _, err := sess.AppendChunk(r.Context(), offset, body, h.storage); err != nil {
		switch {
		case errors.Is(err, services.ErrOffsetMismatch):
			writeError(w, http.StatusConflict, "upload offset mismatch")
		case errors.Is(err, services.ErrAlreadyComplete):
			writeError(w, http.StatusConflict, "upload already complete")
		default:
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("append failed: %v", err))
		}
		return
	}

	newOffset := sess.Offset()

	if sess.Status == services.StatusComplete {
		if err := h.storage.Finalize(r.Context(), sess.Key()); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("finalize failed: %v", err))
			return
		}

		sum := sess.Checksum()
		if existingKey, ok := h.dedup.Lookup(sum); ok {
			_ = h.storage.Delete(r.Context(), sess.Key())
			h.sessions.Delete(id)
			metrics.UploadsDeduplicated.Add(1)
			w.Header().Set("Upload-Offset", strconv.FormatInt(newOffset, 10))
			writeJSON(w, http.StatusOK, uploadResponse{
				ID:           existingKey,
				Status:       "complete",
				Deduplicated: true,
			})
			return
		}

		h.dedup.Register(sum, sess.Key())
		metrics.UploadsCompleted.Add(1)

		// Publish asynchronously — workers handle thumbnail generation and
		// virus scanning without holding up the HTTP response.
		_ = h.pub.Publish(r.Context(), events.UploadComplete{
			ID:       sess.ID,
			Key:      sess.Key(),
			Checksum: sum,
			Size:     newOffset,
		})
	}

	w.Header().Set("Upload-Offset", strconv.FormatInt(newOffset, 10))
	w.WriteHeader(http.StatusNoContent)
}

// HeadUpload handles HEAD /uploads/{id}.
// Returns Upload-Offset so the client knows where to resume.
func (h *UploadHandler) HeadUpload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sess, err := h.sessions.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}
	w.Header().Set("Upload-Offset", strconv.FormatInt(sess.Offset(), 10))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
}

// WriteUpload handles PUT /uploads/{id} — single-shot, non-resumable.
func (h *UploadHandler) WriteUpload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mime, body, err := validate.PeekMIME(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("peek failed: %v", err))
		return
	}
	if err := validate.CheckType(mime, h.allowedTypes); err != nil {
		writeError(w, http.StatusUnsupportedMediaType, err.Error())
		return
	}
	if err := h.storage.Store(r.Context(), id, body); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("upload failed: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, uploadResponse{ID: id, Status: "complete"})
}

// GetUpload handles GET /uploads/{id}.
func (h *UploadHandler) GetUpload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	size, err := h.storage.Stat(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}
	// Include scan status if the session is still live.
	var scanStatus string
	if sess, err := h.sessions.Get(id); err == nil {
		scanStatus = string(sess.ScanStatus)
	}
	writeJSON(w, http.StatusOK, uploadResponse{
		ID:         id,
		Status:     "complete",
		ScanStatus: scanStatus,
		Size:       size,
	})
}

func parseOffset(r *http.Request) (int64, error) {
	s := r.Header.Get("Upload-Offset")
	if s == "" {
		return 0, fmt.Errorf("Upload-Offset header required")
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("invalid Upload-Offset: %q", s)
	}
	return v, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
