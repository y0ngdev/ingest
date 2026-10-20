package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"

	"uuid"

	"ingest/internal/storage"
)

type Status string

const (
	StatusPending  Status = "pending"
	StatusComplete Status = "complete"
)

// ScanStatus tracks the async virus-scan lifecycle for a completed upload.
type ScanStatus string

const (
	ScanStatusNone        ScanStatus = ""
	ScanStatusScanning    ScanStatus = "scanning"
	ScanStatusClean       ScanStatus = "clean"
	ScanStatusQuarantined ScanStatus = "quarantined"
	ScanStatusError       ScanStatus = "error" // all retry attempts failed
)

var (
	ErrNotFound        = errors.New("session not found")
	ErrOffsetMismatch  = errors.New("upload offset mismatch")
	ErrAlreadyComplete = errors.New("upload already complete")
)

type Session struct {
	ID         string
	TotalSize  int64
	received   int64
	hash       hash.Hash
	Status     Status
	ScanStatus ScanStatus
	mu         sync.Mutex
	key        string
}

func newSession(id string, totalSize int64, key string) *Session {
	return &Session{
		ID:        id,
		TotalSize: totalSize,
		hash:      sha256.New(),
		Status:    StatusPending,
		key:       key,
	}
}

// Offset returns the number of bytes received so far.
func (s *Session) Offset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received
}

// Key returns the storage key for this session.
func (s *Session) Key() string { return s.key }

// AppendChunk validates offset, then pipes r through a TeeReader so bytes
// flow to backend.Append and the running SHA-256 hash simultaneously —
// equivalent to io.MultiWriter(backendWriter, s.hash) but without needing a
// concrete writer value.
func (s *Session) AppendChunk(ctx context.Context, offset int64, r io.Reader, b storage.Backend) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Status == StatusComplete {
		return 0, ErrAlreadyComplete
	}
	if offset != s.received {
		return 0, ErrOffsetMismatch
	}

	// TeeReader feeds the same bytes into s.hash while backend.Append drains
	// them to disk (or S3). One pass, zero extra allocations.
	n, err := b.Append(ctx, s.key, offset, io.TeeReader(r, s.hash))
	if err != nil {
		return n, err
	}

	s.received += n
	if s.TotalSize > 0 && s.received >= s.TotalSize {
		s.Status = StatusComplete
	}
	return n, nil
}

// SetScanStatus updates the scan status under the session lock.
// Called by the async scan worker after ClamAV finishes.
func (s *Session) SetScanStatus(st ScanStatus) {
	s.mu.Lock()
	s.ScanStatus = st
	s.mu.Unlock()
}

// Checksum returns the hex-encoded SHA-256 of all bytes received so far.
// Meaningful only after Status == StatusComplete.
func (s *Session) Checksum() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return hex.EncodeToString(s.hash.Sum(nil))
}

// Store is an in-memory registry of active upload sessions.
type Store struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewStore() *Store {
	return &Store{sessions: make(map[string]*Session)}
}

// Create mints a new session, registers it, and returns it.
func (st *Store) Create(totalSize int64) *Session {
	id := uuid.New().String()
	s := newSession(id, totalSize, id)
	st.mu.Lock()
	st.sessions[id] = s
	st.mu.Unlock()
	return s
}

// Get returns the session for id, or ErrNotFound.
func (st *Store) Get(id string) (*Session, error) {
	st.mu.RLock()
	s, ok := st.sessions[id]
	st.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return s, nil
}

// Delete removes a session from the store.
func (st *Store) Delete(id string) {
	st.mu.Lock()
	delete(st.sessions, id)
	st.mu.Unlock()
}
