package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"ingest/internal/dedup"
	"ingest/internal/services"
	"ingest/internal/storage"
)

// UploadHandler wires the upload endpoints to a Backend, a session Store,
// and a dedup Index.
type UploadHandler struct {
	storage  storage.Backend
	sessions *services.Store
	dedup    *dedup.Index
}

func New(b storage.Backend, sessions *services.Store, idx *dedup.Index) *UploadHandler {
	return &UploadHandler{storage: b, sessions: sessions, dedup: idx}
}

type uploadResponse struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Size        int64  `json:"size,omitempty"`
	Deduplicated bool  `json:"deduplicated,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// CreateUpload handles POST /uploads.
// Reads Upload-Length to size the session; 0 means unknown.
func (h *UploadHandler) CreateUpload(w http.ResponseWriter, r *http.Request) {
	var totalSize int64
	if s := r.Header.Get("Upload-Length"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid Upload-Length")
			return
		}
		totalSize = n
	}
	sess := h.sessions.Create(totalSize)
	w.Header().Set("Location", "/uploads/"+sess.ID)
	writeJSON(w, http.StatusCreated, uploadResponse{ID: sess.ID, Status: string(services.StatusPending)})
}

// AppendChunk handles PATCH /uploads/{id}.
// Upload-Offset header must match the session's current received byte count.
// On completion: calls Finalize, checks the dedup index, deletes the new file
// and returns the canonical ID if an identical upload already exists.
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

	if _, err := sess.AppendChunk(r.Context(), offset, r.Body, h.storage); err != nil {
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
			// Duplicate content: discard the just-written file and hand back
			// the canonical storage key from the first upload.
			_ = h.storage.Delete(r.Context(), sess.Key())
			h.sessions.Delete(id)
			w.Header().Set("Upload-Offset", strconv.FormatInt(newOffset, 10))
			writeJSON(w, http.StatusOK, uploadResponse{
				ID:           existingKey,
				Status:       "complete",
				Deduplicated: true,
			})
			return
		}
		h.dedup.Register(sum, sess.Key())
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
	if err := h.storage.Store(r.Context(), id, r.Body); err != nil {
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
	writeJSON(w, http.StatusOK, uploadResponse{ID: id, Status: "complete", Size: size})
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
