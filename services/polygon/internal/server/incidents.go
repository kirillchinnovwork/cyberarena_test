package server

import (
	"context"
	"errors"
	"strings"

	pb "gis/polygon/api/polygon/v1"
	"gis/polygon/services/polygon/internal/storage"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *PolygonServer) GetIncidents(ctx context.Context, req *pb.GetIncidentsRequest) (*pb.GetIncidentsResponse, error) {
	if req.GetPolygonId() == "" {
		return nil, status.Error(codes.InvalidArgument, "polygon_id required")
	}
	pid, err := uuid.Parse(req.GetPolygonId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid polygon_id")
	}
	list, err := s.repo.ListIncidents(ctx, pid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list incidents: %v", err)
	}
	resp := &pb.GetIncidentsResponse{}
	
	_, teamIDStr, _ := s.extractAuth(ctx)
	var currentTeamID uuid.UUID
	var currentTeamType *int32
	if teamIDStr != "" {
		if tid, err := uuid.Parse(teamIDStr); err == nil {
			currentTeamID = tid
			if tm, err := s.repo.GetTeam(ctx, tid); err == nil {
				ct := tm.Type
				currentTeamType = &ct
			}
		}
	}
	incidentIDs := make([]uuid.UUID, 0, len(list))
	for _, in := range list {
		incidentIDs = append(incidentIDs, in.ID)
	}
	redStatuses, _ := s.repo.GetLatestReportStatusesByType(ctx, incidentIDs, int32(pb.TeamType_TEAM_TYPE_RED))
	blueStatuses, _ := s.repo.GetLatestReportStatusesByType(ctx, incidentIDs, int32(pb.TeamType_TEAM_TYPE_BLUE))
	redMap := map[uuid.UUID][]storage.LatestReportStatus{}
	for _, rs := range redStatuses {
		redMap[rs.IncidentID] = append(redMap[rs.IncidentID], rs)
	}
	blueMap := map[uuid.UUID][]storage.LatestReportStatus{}
	for _, bs := range blueStatuses {
		blueMap[bs.IncidentID] = append(blueMap[bs.IncidentID], bs)
	}
	latestRed := map[uuid.UUID]storage.LatestReportStatus{}
	for incID, arr := range redMap {
		var best storage.LatestReportStatus
		for i, v := range arr {
			if i == 0 || v.CreatedAt.After(best.CreatedAt) {
				best = v
			}
		}
		latestRed[incID] = best
	}
	latestBlue := map[uuid.UUID]storage.LatestReportStatus{}
	for incID, arr := range blueMap {
		var best storage.LatestReportStatus
		for i, v := range arr {
			if i == 0 || v.CreatedAt.After(best.CreatedAt) {
				best = v
			}
		}
		latestBlue[incID] = best
	}
	acceptedRedTeams, _ := s.repo.GetAcceptedReportTeamIDs(ctx, incidentIDs, int32(pb.TeamType_TEAM_TYPE_RED))
	for _, in := range list {
		iv := &pb.IncidentView{Id: in.ID.String(), Name: in.Name, Description: in.Description}
		if currentTeamType != nil && *currentTeamType == int32(pb.TeamType_TEAM_TYPE_RED) {
			if st, err := s.repo.GetLatestReportStatusForTeam(ctx, in.ID, currentTeamID); err == nil {
				iv.RedReportStatus = pb.ReportStatus(st)
			}
			for _, tid := range acceptedRedTeams[in.ID] {
				if tid == currentTeamID {
					if lr, ok := latestBlue[in.ID]; ok {
						iv.BlueReportStatus = pb.ReportStatus(lr.Status)
					}
					break
				}
			}
		} else if currentTeamType != nil && *currentTeamType == int32(pb.TeamType_TEAM_TYPE_BLUE) {
			if st, err := s.repo.GetLatestReportStatusForTeam(ctx, in.ID, currentTeamID); err == nil {
				iv.BlueReportStatus = pb.ReportStatus(st)
			}
			if lr, ok := latestRed[in.ID]; ok {
				iv.RedReportStatus = pb.ReportStatus(lr.Status)
			}
		}
		resp.Incidents = append(resp.Incidents, iv)
	}
	return resp, nil
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
	id := uuid.New()
	if err := s.repo.CreateIncident(ctx, id, pid, strings.TrimSpace(req.GetName()), req.GetDescription()); err != nil {
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}
	return &pb.Incident{Id: id.String(), Name: req.GetName(), Description: req.GetDescription()}, nil
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
	if req.Name != "" {
		v := strings.TrimSpace(req.Name)
		namePtr = &v
	}
	if req.Description != "" {
		v := req.Description
		descPtr = &v
	}
	if err := s.repo.UpdateIncident(ctx, id, namePtr, descPtr); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "incident not found")
		}
		return nil, status.Errorf(codes.Internal, "update: %v", err)
	}
	
	return &pb.Incident{Id: id.String(), Name: derefOr(namePtr, ""), Description: derefOr(descPtr, "")}, nil
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