package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
)

type Local struct {
	dir string
}

func New(dir string) (*Local, error) {

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &Local{dir: dir}, nil
}

func (l *Local) Store(ctx context.Context, key string, r io.Reader) error {

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
