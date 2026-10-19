package services

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending  Status = "pending"
	StatusComplete Status = "complete"
)

var (
	ErrNotFound        = errors.New("session not found")
	ErrOffsetMismatch  = errors.New("upload offset mismatch")
	ErrAlreadyComplete = errors.New("upload already complete")
)

type Session struct {
	ID        string
	TotalSize int64
	received  int64
	hash      hash.Hash
	Status    Status
	mu        sync.Mutex
	key       string
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
func (s *Session) Key() string {
	return s.key
}

// AppendChunk writes r into dst at the given byte offset, updating the running
// hash and received count. Returns ErrOffsetMismatch when offset doesn't match
// the current position, preventing gaps or overlapping writes.
func (s *Session) AppendChunk(offset int64, r io.Reader, dst io.WriterAt) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Status == StatusComplete {
		return 0, ErrAlreadyComplete
	}
	if offset != s.received {
		return 0, ErrOffsetMismatch
	}

	n, err := copyAt(io.TeeReader(r, s.hash), dst, offset)
	if err != nil {
		return n, err
	}

	s.received += n
	if s.TotalSize > 0 && s.received >= s.TotalSize {
		s.Status = StatusComplete
	}
	return n, nil
}

// Checksum returns the hex-encoded SHA-256 of all bytes received so far.
// Only meaningful once Status == StatusComplete.
func (s *Session) Checksum() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return hex.EncodeToString(s.hash.Sum(nil))
}

// copyAt drains r into w at the given offset, returning the byte count.
func copyAt(r io.Reader, w io.WriterAt, offset int64) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64
	for {
		nr, readErr := r.Read(buf)
		if nr > 0 {
			nw, writeErr := w.WriteAt(buf[:nr], offset+written)
			written += int64(nw)
			if writeErr != nil {
				return written, writeErr
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return written, readErr
		}
	}
	return written, nil
}

// Store is an in-memory registry of active upload sessions.
type Store struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewStore() *Store {
	return &Store{sessions: make(map[string]*Session)}
}

// Create mints a new session with a UUID, registers it, and returns it.
func (st *Store) Create(totalSize int64) *Session {
	id := uuid.NewString()
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
