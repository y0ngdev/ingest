package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type LocalStorage struct {
	dir string
}

// New creates the upload directory if it does not exist.
func New(dir string) (*LocalStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("storage: create dir %q: %w", dir, err)
	}
	return &LocalStorage{dir: dir}, nil
}

// Store creates a file at dir/key and drains r into it via io.Copy.
// Nothing is loaded into memory — bytes flow straight from reader to disk.
func (l *LocalStorage) Store(_ context.Context, key string, r io.Reader) error {
	f, err := os.Create(filepath.Join(l.dir, key))
	if err != nil {
		return fmt.Errorf("storage: create file: %w", err)
	}
	defer f.Close()

	if _, err = io.Copy(f, r); err != nil {
		// delete file if copy fails
		_ = os.Remove(f.Name())
		return fmt.Errorf("storage: write file: %w", err)
	}
	return nil
}

// Retrieve opens the file at dir/key. The caller must close the returned
func (l *LocalStorage) Retrieve(_ context.Context, key string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(l.dir, key))
}

// Stat returns the size in bytes of the file at dir/key.
func (l *LocalStorage) Stat(_ context.Context, key string) (int64, error) {
	info, err := os.Stat(filepath.Join(l.dir, key))
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Delete removes the file at dir/key.
func (l *LocalStorage) Delete(_ context.Context, key string) error {
	return os.Remove(filepath.Join(l.dir, key))
}
