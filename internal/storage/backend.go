package storage

import (
	"context"
	"io"
)

type Backend interface {
	Store(ctx context.Context, key string, r io.Reader) error
	Retrieve(ctx context.Context, key string) (io.ReadCloser, error)
	Stat(ctx context.Context, key string) (int64, error)
	Delete(ctx context.Context, key string) error
}
