package server

import (
	"context"
	"errors"
	"io"
	"strings"

	pb "gis/polygon/api/polygon/v1"
	"gis/polygon/services/polygon/internal/storage"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	httpbody "google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *PolygonServer) GetPolygons(ctx context.Context, _ *emptypb.Empty) (*pb.GetPolygonsResponse, error) {
	polys, err := s.repo.ListPolygonsWithIncidents(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list polygons: %v", err)
	}
	resp := &pb.GetPolygonsResponse{}
	
	var allIncidentIDs []uuid.UUID
	for _, p := range polys {
		for _, in := range p.Incidents {
			allIncidentIDs = append(allIncidentIDs, in.ID)
		}
	}
	
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
	
	redStatuses, _ := s.repo.GetLatestReportStatusesByType(ctx, allIncidentIDs, int32(pb.TeamType_TEAM_TYPE_RED))
	blueStatuses, _ := s.repo.GetLatestReportStatusesByType(ctx, allIncidentIDs, int32(pb.TeamType_TEAM_TYPE_BLUE))
	
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
	
	acceptedRedTeams, _ := s.repo.GetAcceptedReportTeamIDs(ctx, allIncidentIDs, int32(pb.TeamType_TEAM_TYPE_RED))
	acceptedBlueTeams, _ := s.repo.GetAcceptedReportTeamIDs(ctx, allIncidentIDs, int32(pb.TeamType_TEAM_TYPE_BLUE))
	for _, p := range polys {
		pv := &pb.PolygonView{Id: p.ID.String(), Name: p.Name, Description: p.Description, CoverUrl: p.CoverURL}
		for _, in := range p.Incidents {
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
			pv.Incidents = append(pv.Incidents, iv)
		}
		resp.Polygons = append(resp.Polygons, pv)
	}
	_ = acceptedBlueTeams
	return resp, nil
}

func (s *PolygonServer) DownloadPolygonCover(req *pb.DownloadPolygonCoverRequest, stream pb.PolygonClientService_DownloadPolygonCoverServer) error {
	if req.GetPolygonId() == "" {
		return status.Error(codes.InvalidArgument, "polygon_id required")
	}
	pid, err := uuid.Parse(req.GetPolygonId())
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid polygon_id")
	}
	if s.s3 == nil {
		return status.Error(codes.FailedPrecondition, "s3 not configured")
	}
	pol, err := s.repo.GetPolygon(stream.Context(), pid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return status.Error(codes.NotFound, "polygon not found")
		}
		return status.Errorf(codes.Internal, "get polygon: %v", err)
	}
	if pol.CoverKey == "" {
		return status.Error(codes.NotFound, "cover not set")
	}
	obj, _, ct, err := s.s3.GetObject(stream.Context(), pol.CoverKey)
	if err != nil {
		return status.Errorf(codes.Internal, "s3 get: %v", err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return status.Errorf(codes.Internal, "read: %v", err)
	}
	return stream.Send(&httpbody.HttpBody{ContentType: ct, Data: data})
}

func (s *PolygonServer) CreatePolygon(ctx context.Context, req *pb.CreatePolygonRequest) (*pb.Polygon, error) {
	if strings.TrimSpace(req.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	id := uuid.New()
	if err := s.repo.CreatePolygon(ctx, id, strings.TrimSpace(req.GetName()), req.GetDescription(), req.GetCoverUrl(), ""); err != nil {
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}
	return &pb.Polygon{Id: id.String(), Name: req.GetName(), Description: req.GetDescription(), CoverUrl: req.GetCoverUrl()}, nil
}
func (s *PolygonServer) EditPolygon(ctx context.Context, req *pb.EditPolygonRequest) (*pb.Polygon, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	var namePtr, descPtr, coverPtr *string
	if req.Name != "" {
		v := strings.TrimSpace(req.Name)
		namePtr = &v
	}
	if req.Description != "" {
		v := req.Description
		descPtr = &v
	}
	if req.CoverUrl != "" {
		v := req.CoverUrl
		coverPtr = &v
	}
	if err := s.repo.UpdatePolygon(ctx, id, namePtr, descPtr, coverPtr, nil); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "polygon not found")
		}
		return nil, status.Errorf(codes.Internal, "update: %v", err)
	}
	p, err := s.repo.GetPolygon(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return &pb.Polygon{Id: p.ID.String(), Name: p.Name, Description: p.Description, CoverUrl: p.CoverURL}, nil
}
func (s *PolygonServer) DeletePolygon(ctx context.Context, req *pb.DeletePolygonRequest) (*emptypb.Empty, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	if err := s.repo.DeletePolygon(ctx, id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "polygon not found")
		}
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *PolygonServer) UploadPolygonCover(ctx context.Context, req *pb.UploadPolygonCoverRequest) (*pb.UploadPolygonCoverResponse, error) {
	if req.GetPolygonId() == "" {
		return nil, status.Error(codes.InvalidArgument, "polygon_id required")
	}
	if req.Cover == nil {
		return nil, status.Error(codes.InvalidArgument, "cover body required")
	}
	pid, err := uuid.Parse(req.GetPolygonId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid polygon_id")
	}
	if s.s3 == nil {
		return nil, status.Error(codes.FailedPrecondition, "s3 not configured")
	}
	data := req.Cover.GetData()
	if len(data) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty cover")
	}
	key := s.s3.ObjectKey("covers", pid.String(), "cover.bin")
	url, size, err := s.s3.PutBytes(ctx, key, data, req.Cover.GetContentType())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "s3 put: %v", err)
	}
	
	urlPtr := &url
	keyPtr := &key
	if err := s.repo.UpdatePolygon(ctx, pid, nil, nil, urlPtr, keyPtr); err != nil {
		return nil, status.Errorf(codes.Internal, "update polygon: %v", err)
	}
	return &pb.UploadPolygonCoverResponse{Cover: &pb.PolygonCoverMeta{Url: url, ContentType: req.Cover.GetContentType(), Size: size}}, nil
}