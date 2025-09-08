package media

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Storage struct {
	client     *minio.Client
	bucket     string
	publicBase string
}

func NewS3(ctx context.Context, endpoint, accessKey, secretKey, bucket string, useSSL bool, publicBase string) (*S3Storage, error) {
	cl, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: useSSL})
	if err != nil {
		return nil, err
	}
	
	exists, err := cl.BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := cl.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, err
		}
	}
	return &S3Storage{client: cl, bucket: bucket, publicBase: strings.TrimRight(publicBase, "/")}, nil
}

func (s *S3Storage) buildPublicURL(objectKey string) string {
	if s.publicBase != "" {
		u, _ := url.Parse(s.publicBase)
		u.Path = strings.TrimRight(u.Path, "/") + "/" + objectKey
		return u.String()
	}
	
	return objectKey
}

func (s *S3Storage) PutObject(ctx context.Context, objectKey string, r io.Reader, size int64, contentType string) (string, error) {
	opts := minio.PutObjectOptions{ContentType: contentType}
	_, err := s.client.PutObject(ctx, s.bucket, objectKey, r, size, opts)
	if err != nil {
		return "", err
	}
	return s.buildPublicURL(objectKey), nil
}

func (s *S3Storage) PutBytes(ctx context.Context, objectKey string, data []byte, contentType string) (string, int64, error) {
	u, err := s.PutObject(ctx, objectKey, bytes.NewReader(data), int64(len(data)), contentType)
	return u, int64(len(data)), err
}

func (s *S3Storage) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, int64, string, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, "", err
	}
	
	st, err := obj.Stat()
	if err != nil {
		obj.Close()
		return nil, 0, "", err
	}
	ct := st.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	return obj, st.Size, ct, nil
}

func (s *S3Storage) DeleteObject(ctx context.Context, objectKey string) error {
	return s.client.RemoveObject(ctx, s.bucket, objectKey, minio.RemoveObjectOptions{})
}

func (s *S3Storage) ObjectKey(prefix, id string, filename string) string {
	
	key := strings.Trim(prefix, "/") + "/" + id
	if filename != "" {
		key = fmt.Sprintf("%s/%s", key, filename)
	}
	return key
}
