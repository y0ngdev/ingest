package storage

import (
	"context"
	"io"
)

type Backend interface {
	// Single-shot write — used for non-chunked uploads.
	Store(ctx context.Context, key string, r io.Reader) error
	Retrieve(ctx context.Context, key string) (io.ReadCloser, error)
	Stat(ctx context.Context, key string) (int64, error)
	Delete(ctx context.Context, key string) error

	// Chunked write — called once per PATCH. offset must equal the number of
	// bytes already written; returns the number of bytes accepted.
	Append(ctx context.Context, key string, offset int64, r io.Reader) (int64, error)
	// Finalize commits a chunked upload. No-op for local disk; triggers
	// CompleteMultipartUpload for S3.
	Finalize(ctx context.Context, key string) error
}
