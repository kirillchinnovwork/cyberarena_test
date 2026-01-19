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
	var teamIDs []uuid.UUID
	if uid, _, err := s.extractAuth(ctx); err == nil && uid != "" {
		if u, err := uuid.Parse(uid); err == nil {
			userIDPtr = &u
			// Список команд пользователя
			teams, err := s.repo.ListUserTeams(ctx, u)
			if err == nil { // игнорируем ошибку, чтобы не прерывать выдачу публичных
				for _, t := range teams {
					teamIDs = append(teamIDs, t.ID)
				}
			}
		}
	}
	list, err := s.repo.ListInitialItems(ctx, userIDPtr, teamIDs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list initial: %v", err)
	}
	resp := &pb.GetInitialItemsResponse{}
	for _, it := range list {
		var uidStr string
		if it.UserID != nil {
			uidStr = it.UserID.String()
		}
		var tidStr string
		if it.TeamID != nil {
			tidStr = it.TeamID.String()
		}
		resp.InitialItems = append(resp.InitialItems, &pb.InitialItem{Id: it.ID.String(), Name: it.Name, Description: it.Description, FilesUrls: it.Files, UserId: uidStr, TeamId: tidStr})
	}
	return resp, nil
}

func (s *PolygonServer) CreateInitialItem(ctx context.Context, req *pb.CreateInitialItemRequest) (*pb.InitialItem, error) {
	if strings.TrimSpace(req.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	id := uuid.New()
	var userIDPtr *uuid.UUID
	var teamIDPtr *uuid.UUID
	if req.GetUserId() != "" {
		if u, err := uuid.Parse(req.GetUserId()); err == nil {
			userIDPtr = &u
		} else {
			return nil, status.Error(codes.InvalidArgument, "invalid user_id")
		}
	}
	// прямой доступ к полю team_id (после регенерации кода появится геттер)
	if req.TeamId != "" {
		if t, err := uuid.Parse(req.TeamId); err == nil {
			teamIDPtr = &t
		} else {
			return nil, status.Error(codes.InvalidArgument, "invalid team_id")
		}
	}
	if err := s.repo.CreateInitialItem(ctx, id, strings.TrimSpace(req.GetName()), req.GetDescription(), req.GetFilesUrls(), userIDPtr, teamIDPtr); err != nil {
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}
	var uidStr string
	if userIDPtr != nil {
		uidStr = userIDPtr.String()
	}
	var tidStr string
	if teamIDPtr != nil {
		tidStr = teamIDPtr.String()
	}
	return &pb.InitialItem{Id: id.String(), Name: req.GetName(), Description: req.GetDescription(), FilesUrls: req.GetFilesUrls(), UserId: uidStr, TeamId: tidStr}, nil
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
	teamSet := false
	var teamPtr *uuid.UUID
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
	if req.UserId != "" || (req.UserId == "" && req.UserId != "") { // логика сохранена, хотя условие всегда false для второй части
		userSet = true
		if req.UserId != "" {
			if u, err := uuid.Parse(req.UserId); err == nil {
				userPtr = &u
			} else {
				return nil, status.Error(codes.InvalidArgument, "invalid user_id")
			}
		}
	}
	if req.TeamId != "" { // установка / сброс team_id (пустая строка = сделать публичным)
		teamSet = true
		if req.TeamId != "" {
			if t, err := uuid.Parse(req.TeamId); err == nil {
				teamPtr = &t
			} else {
				return nil, status.Error(codes.InvalidArgument, "invalid team_id")
			}
		}
	}
	if err := s.repo.UpdateInitialItem(ctx, iid, namePtr, descPtr, filesPtr, userSet, userPtr, teamSet, teamPtr); err != nil {
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
	var tidStr string
	if it.TeamID != nil {
		tidStr = it.TeamID.String()
	}
	return &pb.InitialItem{Id: it.ID.String(), Name: it.Name, Description: it.Description, FilesUrls: it.Files, UserId: uidStr, TeamId: tidStr}, nil
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
