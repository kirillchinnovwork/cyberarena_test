package server

import (
	"context"
	"log"
	"net"

	newsv1 "gis/polygon/api/news/v1"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type NewsServer struct {
	newsv1.UnimplementedNewsClientServiceServer
	newsv1.UnimplementedNewsAdminServiceServer
}

func (n *NewsServer) CreateNews(ctx context.Context, req *newsv1.CreateNewsRequest) (*newsv1.CreateNewsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) UpdateNews(ctx context.Context, req *newsv1.UpdateNewsRequest) (*newsv1.UpdateNewsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) DeleteNews(ctx context.Context, req *newsv1.DeleteNewsRequest) (*emptypb.Empty, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) PublishNews(ctx context.Context, req *newsv1.PublishNewsRequest) (*newsv1.PublishNewsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) UnpublishNews(ctx context.Context, req *newsv1.UnpublishNewsRequest) (*newsv1.UnpublishNewsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) GetAllNewsList(ctx context.Context, req *newsv1.GetNewsListRequest) (*newsv1.GetNewsListResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) GetAnyNews(ctx context.Context, req *newsv1.GetNewsRequest) (*newsv1.GetNewsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) DeleteAttachment(ctx context.Context, req *newsv1.DeleteAttachmentRequest) (*emptypb.Empty, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) GetAttachments(ctx context.Context, req *newsv1.GetAttachmentsRequest) (*newsv1.GetAttachmentsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) mustEmbedUnimplementedNewsAdminServiceServer() {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) GetNewsList(ctx context.Context, req *newsv1.GetNewsListRequest) (*newsv1.GetNewsListResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NewsServer) GetNews(ctx context.Context, req *newsv1.GetNewsRequest) (*newsv1.GetNewsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func New() *NewsServer { return &NewsServer{} }

func RunGRPC(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	newsServer := New()
	newsv1.RegisterNewsClientServiceServer(grpcServer, newsServer)
	newsv1.RegisterNewsAdminServiceServer(grpcServer, newsServer)
	log.Printf("news gRPC listening on %s", addr)
	return grpcServer.Serve(lis)
}
