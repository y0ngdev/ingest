package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// multipartUpload holds the state for an in-progress multipart upload.
type multipartUpload struct {
	uploadID string
	parts    []minio.CompletePart
	nextPart int // 1-based part number
}

// S3Config configures an S3-compatible backend.
// Works with AWS S3, MinIO, Cloudflare R2, Backblaze B2, and any service
// that speaks the S3 REST API.
type S3Config struct {
	// Endpoint is the host[:port] of the object store, e.g.:
	//   "localhost:9000"                          (MinIO, local)
	//   "s3.amazonaws.com"                        (AWS S3)
	//   "<account>.r2.cloudflarestorage.com"      (Cloudflare R2)
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	// Secure enables TLS. Set false only for local dev (plain HTTP).
	Secure bool
	// Region is optional; defaults to "us-east-1" when empty.
	Region string
}

// S3Backend implements Backend against any S3-compatible object store.
//
// Chunked lifecycle (PATCH path):
//   Append × N  →  NewMultipartUpload (on first chunk) + PutObjectPart × N
//   Finalize    →  CompleteMultipartUpload
//
// Single-shot lifecycle (PUT path):
//   Store  →  PutObject
type S3Backend struct {
	core   *minio.Core
	bucket string
	mu     sync.Mutex
	active map[string]*multipartUpload // storage key → in-progress upload
}

// NewS3 builds a backend from cfg. Credentials are explicit constructor
// params — no environment-specific credential chain.
func NewS3(cfg S3Config) (*S3Backend, error) {
	core, err := minio.NewCore(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.Secure,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("s3: create client: %w", err)
	}
	return &S3Backend{
		core:   core,
		bucket: cfg.Bucket,
		active: make(map[string]*multipartUpload),
	}, nil
}

// Store uploads the full contents of r in a single PutObject call.
// Pass -1 as size to let the SDK handle unknown-length bodies.
func (b *S3Backend) Store(ctx context.Context, key string, r io.Reader) error {
	_, err := b.core.Client.PutObject(ctx, b.bucket, key, r, -1, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("s3: put %q: %w", key, err)
	}
	return nil
}

func (b *S3Backend) Retrieve(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := b.core.Client.GetObject(ctx, b.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("s3: get %q: %w", key, err)
	}
	return obj, nil
}

func (b *S3Backend) Stat(ctx context.Context, key string) (int64, error) {
	info, err := b.core.Client.StatObject(ctx, b.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("s3: stat %q: %w", key, err)
	}
	return info.Size, nil
}

func (b *S3Backend) Delete(ctx context.Context, key string) error {
	err := b.core.Client.RemoveObject(ctx, b.bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("s3: remove %q: %w", key, err)
	}
	return nil
}

// Append uploads r as the next part of a multipart upload.
// The first call for a given key starts the multipart session;
// subsequent calls add parts in order.
//
// S3 requires each part (except the last) to be at least 5 MB —
// the caller is responsible for sizing chunks appropriately.
func (b *S3Backend) Append(ctx context.Context, key string, _ int64, r io.Reader) (int64, error) {
	mp, partNum, err := b.getOrCreate(ctx, key)
	if err != nil {
		return 0, err
	}

	// Buffer the chunk: PutObjectPart needs a known content length.
	body, err := io.ReadAll(r)
	if err != nil {
		return 0, fmt.Errorf("s3: read chunk for part %d: %w", partNum, err)
	}

	part, err := b.core.PutObjectPart(ctx, b.bucket, key, mp.uploadID, partNum,
		bytes.NewReader(body), int64(len(body)), minio.PutObjectPartOptions{})
	if err != nil {
		return 0, fmt.Errorf("s3: upload part %d of %q: %w", partNum, key, err)
	}

	b.mu.Lock()
	mp.parts = append(mp.parts, minio.CompletePart{
		PartNumber: part.PartNumber,
		ETag:       part.ETag,
	})
	b.mu.Unlock()

	return int64(len(body)), nil
}

// Finalize calls CompleteMultipartUpload, assembling all parts into the
// final object. Must be called exactly once after the last Append.
func (b *S3Backend) Finalize(ctx context.Context, key string) error {
	b.mu.Lock()
	mp, ok := b.active[key]
	if !ok {
		b.mu.Unlock()
		return nil // single-shot Store path — nothing to finalize
	}
	delete(b.active, key)
	b.mu.Unlock()

	_, err := b.core.CompleteMultipartUpload(ctx, b.bucket, key, mp.uploadID, mp.parts, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("s3: complete multipart upload %q: %w", key, err)
	}
	return nil
}

// getOrCreate returns the in-progress multipart for key, creating it on the
// first call, and returns the part number to use for the next PutObjectPart.
func (b *S3Backend) getOrCreate(ctx context.Context, key string) (*multipartUpload, int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	mp, ok := b.active[key]
	if !ok {
		uploadID, err := b.core.NewMultipartUpload(ctx, b.bucket, key, minio.PutObjectOptions{})
		if err != nil {
			return nil, 0, fmt.Errorf("s3: new multipart upload %q: %w", key, err)
		}
		mp = &multipartUpload{uploadID: uploadID, nextPart: 1}
		b.active[key] = mp
	}

	partNum := mp.nextPart
	mp.nextPart++
	return mp, partNum, nil
}
