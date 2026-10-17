package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"ingest/internal/storage"

	"github.com/google/uuid"
)

// UploadHandler wires the three upload endpoints to a Backend.
type UploadHandler struct {
	storage storage.Backend
}

// New returns an initialised UploadHandler.
func New(b storage.Backend) *UploadHandler {
	return &UploadHandler{storage: b}
}

type uploadResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Size   int64  `json:"size"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// CreateUpload handles POST /uploads.
// It mints a fresh UUID for the upload session and returns it.
func (h *UploadHandler) CreateUpload(w http.ResponseWriter, r *http.Request) {
	id := uuid.NewString()
	writeJSON(w, http.StatusCreated, uploadResponse{ID: id, Status: "pending"})
}

// WriteUpload handles PUT /uploads/{id}.
// It streams r.Body directly to storage — no intermediate buffer.
func (h *UploadHandler) WriteUpload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.storage.Store(r.Context(), id, r.Body); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("upload failed: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, uploadResponse{ID: id, Status: "complete"})
}

// GetUpload handles GET /uploads/{id}.
// Phase 1: just confirms the file exists and returns its size from disk.
func (h *UploadHandler) GetUpload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	size, err := h.storage.Stat(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}
	writeJSON(w, http.StatusOK, uploadResponse{ID: id, Status: "complete", Size: size})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
