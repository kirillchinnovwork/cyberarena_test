package server

import (
	"context"
	"log"
	"net"

	usersv1 "gis/polygon/api/users/v1"

	"google.golang.org/grpc"
)

type UsersServer struct {
	usersv1.UnimplementedUsersServiceServer
}

func New() *UsersServer { return &UsersServer{} }

func (s *UsersServer) GetUser(ctx context.Context, req *usersv1.GetUserRequest) (*usersv1.GetUserResponse, error) {
	return &usersv1.GetUserResponse{User: &usersv1.User{Id: req.GetId(), Email: "user@example.com", Name: "Demo"}}, nil
}

func (s *UsersServer) CreateUser(ctx context.Context, req *usersv1.CreateUserRequest) (*usersv1.CreateUserResponse, error) {
	return &usersv1.CreateUserResponse{User: &usersv1.User{Id: "1", Email: req.GetEmail(), Name: req.GetName()}}, nil
}

func RunGRPC(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	usersv1.RegisterUsersServiceServer(grpcServer, New())
	log.Printf("users gRPC listening on %s", addr)
	return grpcServer.Serve(lis)
}
