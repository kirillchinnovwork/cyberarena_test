package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	newsv1 "gis/polygon/api/news/v1"
	"gis/polygon/services/news/internal/media"
	"gis/polygon/services/news/internal/storage"

	gatewayfile "github.com/black-06/grpc-gateway-file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NewsServer struct {
	newsv1.UnimplementedNewsClientServiceServer
	newsv1.UnimplementedNewsAdminServiceServer

	repo *storage.Repo
	s3   *media.S3Storage
}

func (n *NewsServer) CreateNews(ctx context.Context, request *newsv1.CreateNewsRequest) (*newsv1.CreateNewsResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "empty request")
	}
	id := uuid.New()
	now := time.Now().UTC()
	m := &storage.News{
		ID:               id,
		Title:            request.GetTitle(),
		ShortDescription: request.GetShortDescription(),
		CoverURL:         request.GetCoverUrl(),
		Content:          request.GetContent(),
		IsPublished:      false,
		PublishedAt:      nil,
		UpdatedAt:        now,
	}
	if err := n.repo.CreateNews(ctx, m); err != nil {
		return nil, status.Errorf(codes.Internal, "create news: %v", err)
	}
	return &newsv1.CreateNewsResponse{News: toPBNews(m)}, nil
}

func (n *NewsServer) UpdateNews(ctx context.Context, request *newsv1.UpdateNewsRequest) (*newsv1.UpdateNewsResponse, error) {
	if request == nil || request.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	id, err := uuid.Parse(request.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	
	curr, err := n.repo.GetNews(ctx, id)
	if err != nil {
		return nil, mapPgErr(err)
	}
	curr.Title = request.GetTitle()
	curr.ShortDescription = request.GetShortDescription()
	curr.CoverURL = request.GetCoverUrl()
	curr.Content = request.GetContent()
	curr.UpdatedAt = time.Now().UTC()
	if err := n.repo.UpdateNews(ctx, curr); err != nil {
		return nil, mapPgErr(err)
	}
	return &newsv1.UpdateNewsResponse{News: toPBNews(curr)}, nil
}

func (n *NewsServer) DeleteNews(ctx context.Context, request *newsv1.DeleteNewsRequest) (*emptypb.Empty, error) {
	if request == nil || request.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	id, err := uuid.Parse(request.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	if err := n.repo.DeleteNews(ctx, id); err != nil {
		return nil, mapPgErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (n *NewsServer) PublishNews(ctx context.Context, request *newsv1.PublishNewsRequest) (*newsv1.PublishNewsResponse, error) {
	if request == nil || request.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	id, err := uuid.Parse(request.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	nw, err := n.repo.SetPublishState(ctx, id, true, time.Now().UTC())
	if err != nil {
		return nil, mapPgErr(err)
	}
	return &newsv1.PublishNewsResponse{News: toPBNews(nw)}, nil
}

func (n *NewsServer) UnpublishNews(ctx context.Context, request *newsv1.UnpublishNewsRequest) (*newsv1.UnpublishNewsResponse, error) {
	if request == nil || request.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	id, err := uuid.Parse(request.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	nw, err := n.repo.SetPublishState(ctx, id, false, time.Now().UTC())
	if err != nil {
		return nil, mapPgErr(err)
	}
	return &newsv1.UnpublishNewsResponse{News: toPBNews(nw)}, nil
}

func (n *NewsServer) GetAllNewsList(ctx context.Context, request *newsv1.GetNewsListRequest) (*newsv1.GetNewsListResponse, error) {
	page := int(request.GetPage())
	ps := int(request.GetPageSize())
	publishedOnly := request.GetPublishedOnly()
	list, total, err := n.repo.ListNews(ctx, page, ps, publishedOnly)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	return &newsv1.GetNewsListResponse{News: toPBNewsList(list), TotalCount: total, Page: int32(pageOrDefault(page)), PageSize: int32(pageSizeOrDefault(ps))}, nil
}

func (n *NewsServer) GetAnyNews(ctx context.Context, request *newsv1.GetNewsRequest) (*newsv1.GetNewsResponse, error) {
	if request == nil || request.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	id, err := uuid.Parse(request.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	nw, err := n.repo.GetNews(ctx, id)
	if err != nil {
		return nil, mapPgErr(err)
	}
	return &newsv1.GetNewsResponse{News: toPBNews(nw)}, nil
}

func (n *NewsServer) DeleteAttachment(ctx context.Context, request *newsv1.DeleteAttachmentRequest) (*emptypb.Empty, error) {
	if request == nil || request.GetAttachmentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "attachment_id is required")
	}
	id, err := uuid.Parse(request.GetAttachmentId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid attachment_id")
	}
	att, err := n.repo.GetAttachment(ctx, id)
	if err != nil {
		return nil, mapPgErr(err)
	}
	if err := n.s3.DeleteObject(ctx, att.ObjectKey); err != nil {
		return nil, status.Errorf(codes.Internal, "s3 delete: %v", err)
	}
	if err := n.repo.DeleteAttachment(ctx, id); err != nil {
		return nil, mapPgErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (n *NewsServer) GetAttachments(ctx context.Context, request *newsv1.GetAttachmentsRequest) (*newsv1.GetAttachmentsResponse, error) {
	ids := make([]uuid.UUID, 0, len(request.GetAttachmentIds()))
	for _, s := range request.GetAttachmentIds() {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid id: %s", s)
		}
		ids = append(ids, id)
	}
	list, err := n.repo.GetAttachments(ctx, ids)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "db: %v", err)
	}
	res := &newsv1.GetAttachmentsResponse{Attachments: make([]*newsv1.NewsAttachment, 0, len(list))}
	for _, a := range list {
		res.Attachments = append(res.Attachments, toPBAtt(a))
	}
	return res, nil
}

func (n *NewsServer) GetNewsList(ctx context.Context, request *newsv1.GetNewsListRequest) (*newsv1.GetNewsListResponse, error) {
	page := int(request.GetPage())
	ps := int(request.GetPageSize())
	list, total, err := n.repo.ListNews(ctx, page, ps, true)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	return &newsv1.GetNewsListResponse{News: toPBNewsList(list), TotalCount: total, Page: int32(pageOrDefault(page)), PageSize: int32(pageSizeOrDefault(ps))}, nil
}

func (n *NewsServer) GetNews(ctx context.Context, request *newsv1.GetNewsRequest) (*newsv1.GetNewsResponse, error) {
	if request == nil || request.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	id, err := uuid.Parse(request.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	nw, err := n.repo.GetPublishedNews(ctx, id)
	if err != nil {
		return nil, mapPgErr(err)
	}
	return &newsv1.GetNewsResponse{News: toPBNews(nw)}, nil
}

func (n *NewsServer) UploadAttachment(server newsv1.NewsAdminService_UploadAttachmentServer) error {
	formData, err := gatewayfile.NewFormData(server, 100*1024*1024)
	if err != nil {
		if errors.Is(err, gatewayfile.ErrSizeLimitExceeded) {
			return status.Errorf(codes.InvalidArgument, "size limit exceeded")
		}

		return status.Errorf(codes.Internal, err.Error())
	}
	
	defer formData.RemoveAll()

	fileHeader := formData.FirstFile("file")
	if fileHeader == nil {
		return status.Errorf(codes.InvalidArgument, "missing file for key key1")
	}
	file, err := fileHeader.Open()
	if err != nil {
		log.Println(err)
		return status.Errorf(codes.Internal, "open file: %v", err)
	}

	ctx := server.Context()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, file)
	if err != nil && !errors.Is(err, io.EOF) {
		log.Println(err)
		return status.Errorf(codes.Internal, "read file: %v", err)
	}

	log.Println("upload complete, size:", buf.Len(), "bytes")
	if buf.Len() == 0 {
		log.Println("empty file")
		return status.Error(codes.InvalidArgument, "empty file")
	}
	id := uuid.New()
	key := n.s3.ObjectKey("attachments", id.String(), "")
	_, size, err := n.s3.PutBytes(ctx, key, buf.Bytes(), contentTypeOrDefault(fileHeader.Header.Get("Content-Type")))
	if err != nil {
		return status.Errorf(codes.Internal, "s3 put: %v", err)
	}
	att := &storage.Attachment{ID: id, URL: "/v1/news/attachments/" + id.String(), ContentType: contentTypeOrDefault(fileHeader.Header.Get("Content-Type")), Size: size, ObjectKey: key}
	if err := n.repo.CreateAttachment(ctx, att); err != nil {
		return status.Errorf(codes.Internal, "db: %v", err)
	}
	return server.SendAndClose(&newsv1.UploadAttachmentResponse{Attachment: toPBAtt(att)})
}

func (n *NewsServer) DownloadAttachment(request *newsv1.DownloadAttachmentRequest, server newsv1.NewsClientService_DownloadAttachmentServer) error {
	if request == nil || request.GetId() == "" {
		log.Println("DownloadAttachment: empty request or id")
		return status.Error(codes.InvalidArgument, "id is required")
	}
	id, err := uuid.Parse(request.GetId())
	if err != nil {
		log.Printf("DownloadAttachment: invalid id %s: %v", request.GetId(), err)
		return status.Error(codes.InvalidArgument, "invalid id")
	}
	att, err := n.repo.GetAttachment(server.Context(), id)
	if err != nil {
		log.Printf("DownloadAttachment: get attachment %s: %v", id, err)
		return mapPgErr(err)
	}
	rc, _, ct, err := n.s3.GetObject(server.Context(), att.ObjectKey)
	if err != nil {
		log.Printf("DownloadAttachment: s3 get %s: %v", att.ObjectKey, err)
		return status.Errorf(codes.Internal, "s3 get: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		log.Printf("DownloadAttachment: read %s: %v", att.ObjectKey, err)
		return status.Errorf(codes.Internal, "read: %v", err)
	}

	err = server.Send(&httpbody.HttpBody{
		ContentType: ct,
		Data:        data,
	})
	log.Println(err)

	return err
}

func New(repo *storage.Repo, s3 *media.S3Storage) *NewsServer { return &NewsServer{repo: repo, s3: s3} }

func RunGRPC(addr string) error {
	
	pgDsn := getenv("NEWS_PG_DSN", "postgres://postgres:postgres@localhost:5432/news?sslmode=disable")
	pool, err := pgxpool.New(context.Background(), pgDsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	repo := storage.NewRepo(pool)
	if err := repo.Migrate(context.Background()); err != nil {
		return err
	}
	
	s3Endpoint := getenv("NEWS_S3_ENDPOINT", "localhost:9000")
	s3Access := getenv("NEWS_S3_ACCESS_KEY", "minioadmin")
	s3Secret := getenv("NEWS_S3_SECRET_KEY", "minioadmin")
	s3Bucket := getenv("NEWS_S3_BUCKET", "news")
	useSSL := getenvBool("NEWS_S3_USE_SSL", false)
	publicBase := getenv("NEWS_S3_PUBLIC_BASE", "")
	s3, err := media.NewS3(context.Background(), s3Endpoint, s3Access, s3Secret, s3Bucket, useSSL, publicBase)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	newsServer := New(repo, s3)
	newsv1.RegisterNewsClientServiceServer(grpcServer, newsServer)
	newsv1.RegisterNewsAdminServiceServer(grpcServer, newsServer)
	log.Printf("news gRPC listening on %s", addr)
	return grpcServer.Serve(lis)
}


func toPBNews(n *storage.News) *newsv1.News {
	if n == nil {
		return nil
	}
	pb := &newsv1.News{
		Id:               n.ID.String(),
		Title:            n.Title,
		CoverUrl:         n.CoverURL,
		ShortDescription: n.ShortDescription,
		Content:          n.Content,
		IsPublished:      n.IsPublished,
		UpdatedAt:        timestamppb.New(n.UpdatedAt),
	}
	if n.PublishedAt != nil {
		pb.PublishedAt = timestamppb.New(*n.PublishedAt)
	}
	return pb
}

func toPBNewsList(list []*storage.News) []*newsv1.News {
	res := make([]*newsv1.News, 0, len(list))
	for _, n := range list {
		res = append(res, toPBNews(n))
	}
	return res
}

func toPBAtt(a *storage.Attachment) *newsv1.NewsAttachment {
	if a == nil {
		return nil
	}
	return &newsv1.NewsAttachment{Id: a.ID.String(), Url: a.URL, ContentType: a.ContentType, Size: a.Size}
}

func contentTypeOrDefault(ct string) string {
	if ct == "" {
		return "application/octet-stream"
	}
	return ct
}

func pageOrDefault(p int) int {
	if p <= 0 {
		return 1
	}
	return p
}
func pageSizeOrDefault(ps int) int {
	if ps <= 0 {
		return 20
	}
	return ps
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func getenvBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		b, _ := strconv.ParseBool(v)
		return b
	}
	return def
}

func mapPgErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, "not found")
	}
	
	if err.Error() == "no rows in result set" {
		return status.Error(codes.NotFound, "not found")
	}
	return status.Errorf(codes.Internal, "%v", err)
}
