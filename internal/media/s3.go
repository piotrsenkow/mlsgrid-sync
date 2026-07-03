package media

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Options configures an S3-compatible sink. Credentials come from the
// standard AWS environment/credential chain (AWS_ACCESS_KEY_ID etc.) — never
// from the config file.
type S3Options struct {
	// Endpoint overrides the AWS default for S3-compatible services
	// (MinIO, Cloudflare R2, Backblaze B2). Setting it switches to
	// path-style addressing, which those services expect.
	Endpoint string
	Bucket   string
	// Prefix is prepended to every object key.
	Prefix string
	// Region defaults to us-east-1; S3-compatible endpoints accept any.
	Region string
}

// S3Sink stores objects in an S3 (or S3-compatible) bucket.
type S3Sink struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewS3Sink builds the sink from the default AWS credential chain.
func NewS3Sink(ctx context.Context, o S3Options) (*S3Sink, error) {
	if o.Bucket == "" {
		return nil, errors.New("media: s3 sink bucket is empty")
	}
	region := o.Region
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("media: loading AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(opts *s3.Options) {
		// Header-based checksums keep uploads compatible with S3-compatible
		// services that reject aws-chunked trailer encoding.
		opts.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		if o.Endpoint != "" {
			opts.BaseEndpoint = aws.String(o.Endpoint)
			opts.UsePathStyle = true
		}
	})
	return &S3Sink{client: client, bucket: o.Bucket, prefix: o.Prefix}, nil
}

// Store uploads the object and returns its bucket-relative key.
func (s *S3Sink) Store(ctx context.Context, mediaKey, contentType string, data []byte) (string, error) {
	key := path.Join(s.prefix, objectPath(mediaKey, contentType))
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return "", fmt.Errorf("media: uploading %s: %w", mediaKey, err)
	}
	return key, nil
}

// Remove deletes a stored object (a no-op for keys already gone, per S3
// semantics).
func (s *S3Sink) Remove(ctx context.Context, localPath string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(localPath),
	})
	return err
}
