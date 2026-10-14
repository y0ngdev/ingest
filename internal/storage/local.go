package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
)

type LocalStorage struct {
	dir string
}

func New(dir string) (*LocalStorage, error) {

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &LocalStorage{dir: dir}, nil
}

func (l *LocalStorage) Store(ctx context.Context, key string, r io.Reader) error {

	// Create the file

	file, err := os.Create(filepath.Join(l.dir, key))
	if err != nil {
		return err
	}
	defer file.Close()

	// Copy the reader’s bytes into it
	_, err = io.Copy(file, r)
	if err != nil {
		return err
	}

	return nil
}

func (l *LocalStorage) Retrieve(ctx context.Context, key string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(l.dir, key))
}

func (l *LocalStorage) Delete(ctx context.Context, key string) error {

	return os.Remove(filepath.Join(l.dir, key))
}
