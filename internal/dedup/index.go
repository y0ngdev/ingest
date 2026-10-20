package dedup

import "sync"

// Index maps a SHA-256 hex digest to the storage key of the canonical copy of
// that content. Two uploads of identical bytes will hash to the same digest;
// the second one can reuse the first's storage key instead of writing again.
type Index struct {
	mu      sync.RWMutex
	entries map[string]string // sha256hex → storage key
}

func New() *Index {
	return &Index{entries: make(map[string]string)}
}

// Lookup returns (key, true) if the digest is already known.
func (ix *Index) Lookup(sha256hex string) (string, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	key, ok := ix.entries[sha256hex]
	return key, ok
}

// Register records sha256hex → key. If the digest is already present the
// existing mapping wins — two sessions completing identical content
// concurrently both see the same canonical key.
func (ix *Index) Register(sha256hex, key string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if _, ok := ix.entries[sha256hex]; !ok {
		ix.entries[sha256hex] = key
	}
}
