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

func validatePercent(v int64) (int, error) {
	if v < 0 || v > 100 {
		return 0, errors.New("percent out of range")
	}
	return int(v), nil
}
func (s *PolygonServer) CreateIncident(ctx context.Context, req *pb.CreateIncidentRequest) (*pb.Incident, error) {
	if req.GetPolygonId() == "" {
		return nil, status.Error(codes.InvalidArgument, "polygon_id required")
	}
	pid, err := uuid.Parse(req.GetPolygonId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid polygon_id")
	}
	if strings.TrimSpace(req.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	basePrize := req.GetRedPrize()
	if basePrize < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid red_prize")
	}
	bluePct, err := validatePercent(req.GetBluePrizeProcent())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid blue_prize_procent")
	}
	id := uuid.New()
	if err := s.repo.CreateIncident(ctx, id, pid, strings.TrimSpace(req.GetName()), req.GetDescription(), basePrize, bluePct); err != nil {
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}
	return &pb.Incident{Id: id.String(), Name: req.GetName(), Description: req.GetDescription(), RedPrize: req.GetRedPrize(), BluePrizeProcent: req.GetBluePrizeProcent()}, nil
}
func (s *PolygonServer) EditIncident(ctx context.Context, req *pb.EditIncidentRequest) (*pb.Incident, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	var namePtr, descPtr *string
	var basePrizePtr *int64
	var bluePctPtr *int
	if req.Name != "" {
		v := strings.TrimSpace(req.Name)
		namePtr = &v
	}
	if req.Description != "" {
		v := req.Description
		descPtr = &v
	}
	if req.RedPrize > 0 {
		v := req.RedPrize
		basePrizePtr = &v
	}
	if req.BluePrizeProcent > 0 { 
		v, err := validatePercent(req.BluePrizeProcent)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid blue_prize_procent")
		}
		bluePctPtr = &v
	}
	if err := s.repo.UpdateIncident(ctx, id, namePtr, descPtr, basePrizePtr, bluePctPtr); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "incident not found")
		}
		return nil, status.Errorf(codes.Internal, "update: %v", err)
	}
	return &pb.Incident{Id: id.String(), Name: derefOr(namePtr, ""), Description: derefOr(descPtr, ""), RedPrize: req.GetRedPrize(), BluePrizeProcent: req.GetBluePrizeProcent()}, nil
}
func (s *PolygonServer) DeleteIncident(ctx context.Context, req *pb.DeleteIncidentRequest) (*emptypb.Empty, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	if err := s.repo.DeleteIncident(ctx, id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "incident not found")
		}
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &emptypb.Empty{}, nil
}
