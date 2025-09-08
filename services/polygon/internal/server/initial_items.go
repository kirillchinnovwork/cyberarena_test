package server

import (
	"context"
	"errors"
	"strings"

	pb "gis/polygon/api/polygon/v1"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *PolygonServer) GetInitialItems(ctx context.Context, _ *emptypb.Empty) (*pb.GetInitialItemsResponse, error) {
	var userIDPtr *uuid.UUID
	if uid, _, err := s.extractAuth(ctx); err == nil && uid != "" {
		if u, err := uuid.Parse(uid); err == nil {
			userIDPtr = &u
		}
	}
	list, err := s.repo.ListInitialItems(ctx, userIDPtr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list initial: %v", err)
	}
	resp := &pb.GetInitialItemsResponse{}
	for _, it := range list {
		var uidStr string
		if it.UserID != nil {
			uidStr = it.UserID.String()
		}
		resp.InitialItems = append(resp.InitialItems, &pb.InitialItem{Id: it.ID.String(), Name: it.Name, Description: it.Description, FilesUrls: it.Files, UserId: uidStr})
	}
	return resp, nil
}

func (s *PolygonServer) CreateInitialItem(ctx context.Context, req *pb.CreateInitialItemRequest) (*pb.InitialItem, error) {
	if strings.TrimSpace(req.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	id := uuid.New()
	var userIDPtr *uuid.UUID
	if req.GetUserId() != "" {
		if u, err := uuid.Parse(req.GetUserId()); err == nil {
			userIDPtr = &u
		} else {
			return nil, status.Error(codes.InvalidArgument, "invalid user_id")
		}
	}
	if err := s.repo.CreateInitialItem(ctx, id, strings.TrimSpace(req.GetName()), req.GetDescription(), req.GetFilesUrls(), userIDPtr); err != nil {
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}
	var uidStr string
	if userIDPtr != nil {
		uidStr = userIDPtr.String()
	}
	return &pb.InitialItem{Id: id.String(), Name: req.GetName(), Description: req.GetDescription(), FilesUrls: req.GetFilesUrls(), UserId: uidStr}, nil
}
func (s *PolygonServer) EditInitialItem(ctx context.Context, req *pb.EditInitialItemRequest) (*pb.InitialItem, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	iid, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	var namePtr, descPtr *string
	var filesPtr *[]string
	userSet := false
	var userPtr *uuid.UUID
	if req.Name != "" {
		v := strings.TrimSpace(req.Name)
		namePtr = &v
	}
	if req.Description != "" {
		v := req.Description
		descPtr = &v
	}
	if len(req.FilesUrls) > 0 {
		v := req.FilesUrls
		filesPtr = &v
	}
	if req.UserId != "" || (req.UserId == "" && req.UserId != "") {
		userSet = true
		if req.UserId != "" {
			if u, err := uuid.Parse(req.UserId); err == nil {
				userPtr = &u
			} else {
				return nil, status.Error(codes.InvalidArgument, "invalid user_id")
			}
		}
	}
	if err := s.repo.UpdateInitialItem(ctx, iid, namePtr, descPtr, filesPtr, userSet, userPtr); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "initial item not found")
		}
		return nil, status.Errorf(codes.Internal, "update: %v", err)
	}
	it, err := s.repo.GetInitialItem(ctx, iid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	var uidStr string
	if it.UserID != nil {
		uidStr = it.UserID.String()
	}
	return &pb.InitialItem{Id: it.ID.String(), Name: it.Name, Description: it.Description, FilesUrls: it.Files, UserId: uidStr}, nil
}
func (s *PolygonServer) DeleteInitialItem(ctx context.Context, req *pb.DeleteInitialItemRequest) (*emptypb.Empty, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	if err := s.repo.DeleteInitialItem(ctx, id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "initial item not found")
		}
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &emptypb.Empty{}, nil
}