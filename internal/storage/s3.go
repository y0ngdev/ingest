package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// multipartUpload holds the state for an in-progress S3 multipart upload.
type multipartUpload struct {
	uploadID string
	parts    []types.CompletedPart
	nextPart int32 // 1-based part number
}

// S3Backend implements Backend against an S3 bucket.
//
// Chunked lifecycle (PATCH path):
//   Append × N  →  CreateMultipartUpload (on first chunk) + UploadPart × N
//   Finalize    →  CompleteMultipartUpload
//
// Single-shot lifecycle (PUT path):
//   Store  →  PutObject
type S3Backend struct {
	client *s3.Client
	bucket string
	mu     sync.Mutex
	active map[string]*multipartUpload // storage key → in-progress upload
}

// NewS3 reads AWS credentials and region from the environment / shared config
// (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, ~/.aws/config, etc.)
// and returns a backend pointed at bucket.
func NewS3(bucket string) (*S3Backend, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("s3: load config: %w", err)
	}
	return &S3Backend{
		client: s3.NewFromConfig(cfg),
		bucket: bucket,
		active: make(map[string]*multipartUpload),
	}, nil
}

// Store uploads the full contents of r in a single PutObject call.
func (b *S3Backend) Store(ctx context.Context, key string, r io.Reader) error {
	// PutObject needs a seekable or sized body, so we buffer into memory.
	// For large single-shot uploads the PATCH path (Append+Finalize) is
	// preferred because it streams chunk-by-chunk via multipart upload.
	body, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("s3: read body: %w", err)
	}
	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})
	if err != nil {
		return fmt.Errorf("s3: put object %q: %w", key, err)
	}
	return nil
}

func (b *S3Backend) Retrieve(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3: get object %q: %w", key, err)
	}
	return out.Body, nil
}

func (b *S3Backend) Stat(ctx context.Context, key string) (int64, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("s3: head object %q: %w", key, err)
	}
	if out.ContentLength == nil {
		return 0, nil
	}
	return *out.ContentLength, nil
}

func (b *S3Backend) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3: delete object %q: %w", key, err)
	}
	return nil
}

// Append uploads r as the next part of a multipart upload.
//
// The first call for a given key triggers CreateMultipartUpload; subsequent
// calls add parts. offset is accepted for interface compatibility but S3
// determines physical placement by part number — the session layer ensures
// parts arrive in order.
//
// S3 requires part bodies to be buffered so ContentLength is known up front
// (streaming readers have no known size). Parts ≥ 5 MB are required by S3
// for all but the final part — the caller is responsible for sizing chunks.
func (b *S3Backend) Append(ctx context.Context, key string, _ int64, r io.Reader) (int64, error) {
	mp, partNum, err := b.getOrCreateMultipart(ctx, key)
	if err != nil {
		return 0, err
	}

	body, err := io.ReadAll(r)
	if err != nil {
		return 0, fmt.Errorf("s3: read chunk for part %d: %w", partNum, err)
	}

	out, err := b.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(mp.uploadID),
		PartNumber:    aws.Int32(partNum),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})
	if err != nil {
		return 0, fmt.Errorf("s3: upload part %d for %q: %w", partNum, key, err)
	}

	b.mu.Lock()
	mp.parts = append(mp.parts, types.CompletedPart{
		ETag:       out.ETag,
		PartNumber: aws.Int32(partNum),
	})
	b.mu.Unlock()

	return int64(len(body)), nil
}

// Finalize calls CompleteMultipartUpload, assembling all uploaded parts into
// the final S3 object. Must be called exactly once after the last Append.
func (b *S3Backend) Finalize(ctx context.Context, key string) error {
	b.mu.Lock()
	mp, ok := b.active[key]
	if !ok {
		b.mu.Unlock()
		return nil // single-shot Store path — nothing to finalize
	}
	delete(b.active, key)
	b.mu.Unlock()

	_, err := b.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(b.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(mp.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: mp.parts,
		},
	})
	if err != nil {
		return fmt.Errorf("s3: complete multipart upload %q: %w", key, err)
	}
	return nil
}

// getOrCreateMultipart returns the in-progress upload for key (creating it on
// first call) and the part number to use for the next UploadPart call.
func (b *S3Backend) getOrCreateMultipart(ctx context.Context, key string) (*multipartUpload, int32, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	mp, ok := b.active[key]
	if !ok {
		out, err := b.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, 0, fmt.Errorf("s3: create multipart upload %q: %w", key, err)
		}
		mp = &multipartUpload{uploadID: *out.UploadId, nextPart: 1}
		b.active[key] = mp
	}

	partNum := mp.nextPart
	mp.nextPart++
	return mp, partNum, nil
}
