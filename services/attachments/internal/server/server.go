package server

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	attpb "gis/polygon/api/attachments/v1"
	"gis/polygon/services/attachments/internal/media"

	gatewayfile "github.com/black-06/grpc-gateway-file"
	"github.com/google/uuid"
	httpbody "google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AttachmentMeta struct {
	ID          string
	UserID      string
	ContentType string
	Size        int64
	ObjectKey   string
}

type AttachmentsServer struct {
	attpb.UnimplementedAttachmentsClientServiceServer
	attpb.UnimplementedAttachmentsAdminServiceServer
	s3   *media.S3Storage
	mu   sync.RWMutex
	meta map[string]AttachmentMeta
}

// UploadAttachment (client) — требует user id.
func (s *AttachmentsServer) UploadAttachment(stream attpb.AttachmentsClientService_UploadAttachmentServer) error {
	uid, err := extractUserID(stream.Context())
	if err != nil {
		return err
	}
	formData, err := gatewayfile.NewFormData(stream, 100*1024*1024)
	if err != nil {
		return status.Errorf(codes.Internal, "form: %v", err)
	}
	defer formData.RemoveAll()
	fileHeader := formData.FirstFile("file")
	if fileHeader == nil {
		return status.Error(codes.InvalidArgument, "file field required")
	}
	f, err := fileHeader.Open()
	if err != nil {
		return status.Errorf(codes.Internal, "open: %v", err)
	}
	defer f.Close()
	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, f); err != nil {
		return status.Errorf(codes.Internal, "read: %v", err)
	}
	if buf.Len() == 0 {
		return status.Error(codes.InvalidArgument, "empty file")
	}
	if s.s3 == nil {
		return status.Error(codes.FailedPrecondition, "s3 not configured")
	}
	attID := uuid.New()
	key := s.s3.ObjectKey("attachments", attID.String(), "file")
	ct := contentTypeOrDefault(fileHeader.Header.Get("Content-Type"))
	_, size, err := s.s3.PutBytes(stream.Context(), key, buf.Bytes(), ct)
	if err != nil {
		return status.Errorf(codes.Internal, "s3 put: %v", err)
	}
	s.mu.Lock()
	s.meta[attID.String()] = AttachmentMeta{ID: attID.String(), UserID: uid, ContentType: ct, Size: size, ObjectKey: key}
	s.mu.Unlock()
	return stream.SendAndClose(&attpb.UploadAttachmentResponse{Attachment: &attpb.Attachment{Id: attID.String(), Url: "/v1/attachments/" + attID.String(), ContentType: ct, Size: size, UserId: uid}})
}

// UploadAttachmentAdmin — без user id.
func (s *AttachmentsServer) UploadAttachmentAdmin(stream attpb.AttachmentsAdminService_UploadAttachmentAdminServer) error {
	formData, err := gatewayfile.NewFormData(stream, 100*1024*1024)
	if err != nil {
		return status.Errorf(codes.Internal, "form: %v", err)
	}
	defer formData.RemoveAll()
	fileHeader := formData.FirstFile("file")
	if fileHeader == nil {
		return status.Error(codes.InvalidArgument, "file field required")
	}
	f, err := fileHeader.Open()
	if err != nil {
		return status.Errorf(codes.Internal, "open: %v", err)
	}
	defer f.Close()
	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, f); err != nil {
		return status.Errorf(codes.Internal, "read: %v", err)
	}
	if buf.Len() == 0 {
		return status.Error(codes.InvalidArgument, "empty file")
	}
	if s.s3 == nil {
		return status.Error(codes.FailedPrecondition, "s3 not configured")
	}
	attID := uuid.New()
	key := s.s3.ObjectKey("attachments", attID.String(), "file")
	ct := contentTypeOrDefault(fileHeader.Header.Get("Content-Type"))
	_, size, err := s.s3.PutBytes(stream.Context(), key, buf.Bytes(), ct)
	if err != nil {
		return status.Errorf(codes.Internal, "s3 put: %v", err)
	}
	s.mu.Lock()
	s.meta[attID.String()] = AttachmentMeta{ID: attID.String(), UserID: "", ContentType: ct, Size: size, ObjectKey: key}
	s.mu.Unlock()
	return stream.SendAndClose(&attpb.UploadAttachmentResponse{Attachment: &attpb.Attachment{Id: attID.String(), Url: "/v1/attachments/" + attID.String(), ContentType: ct, Size: size, UserId: ""}})
}

// DownloadAttachment — открыто.
func (s *AttachmentsServer) DownloadAttachment(req *attpb.DownloadAttachmentRequest, stream attpb.AttachmentsClientService_DownloadAttachmentServer) error {
	if req.GetId() == "" {
		return status.Error(codes.InvalidArgument, "id required")
	}
	s.mu.RLock()
	mt, ok := s.meta[req.GetId()]
	s.mu.RUnlock()
	if !ok {
		return status.Error(codes.NotFound, "attachment not found")
	}
	if s.s3 == nil {
		return status.Error(codes.FailedPrecondition, "s3 not configured")
	}
	obj, _, ct, err := s.s3.GetObject(stream.Context(), mt.ObjectKey)
	if err != nil {
		return status.Error(codes.NotFound, "object not found")
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return status.Errorf(codes.Internal, "read: %v", err)
	}
	return stream.Send(&httpbody.HttpBody{ContentType: ct, Data: data})
}

func RunGRPC(addr string) error {
	s3Endpoint := getenv("ATTACHMENTS_S3_ENDPOINT", "localhost:9000")
	s3Access := getenv("ATTACHMENTS_S3_ACCESS_KEY", "minioadmin")
	s3Secret := getenv("ATTACHMENTS_S3_SECRET_KEY", "minioadmin")
	s3Bucket := getenv("ATTACHMENTS_S3_BUCKET", "attachments")
	useSSL := getenv("ATTACHMENTS_S3_USE_SSL", "false") == "true"
	publicBase := getenv("ATTACHMENTS_S3_PUBLIC_BASE", "")
	s3, err := media.NewS3(context.Background(), s3Endpoint, s3Access, s3Secret, s3Bucket, useSSL, publicBase)
	if err != nil {
		log.Printf("s3 init error: %v (continuing without s3)", err)
		s3 = nil
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	srv := &AttachmentsServer{s3: s3, meta: make(map[string]AttachmentMeta)}
	attpb.RegisterAttachmentsClientServiceServer(grpcServer, srv)
	attpb.RegisterAttachmentsAdminServiceServer(grpcServer, srv)
	log.Printf("attachments gRPC listening on %s", addr)
	return grpcServer.Serve(lis)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func extractUserID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "no metadata")
	}
	uid := firstNonEmpty(md.Get("x-user-id"))
	if uid == "" {
		return "", status.Error(codes.Unauthenticated, "no user id metadata")
	}
	return uid, nil
}

func firstNonEmpty(vals []string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func contentTypeOrDefault(ct string) string {
	if strings.TrimSpace(ct) == "" {
		return "application/octet-stream"
	}
	return ct
}
