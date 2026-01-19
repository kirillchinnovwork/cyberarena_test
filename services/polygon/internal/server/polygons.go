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

func (s *PolygonServer) GetRedPolygons(ctx context.Context, _ *emptypb.Empty) (*pb.GetRedPolygonsResponse, error) {
	polys, err := s.repo.ListPolygonsWithIncidents(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list polygons: %v", err)
	}
	_, teamIDStr, _ := s.extractAuth(ctx)
	if teamIDStr == "" {
		return &pb.GetRedPolygonsResponse{}, nil
	}

	if tid, err := uuid.Parse(teamIDStr); err == nil {
		if tm, err := s.repo.GetTeam(ctx, tid); err == nil {
			if tm.Type != int32(pb.TeamType_TEAM_TYPE_RED) {
				return &pb.GetRedPolygonsResponse{}, nil
			}
		}
	}
	var allIncidentIDs []uuid.UUID
	for _, p := range polys {
		for _, in := range p.Incidents {
			allIncidentIDs = append(allIncidentIDs, in.ID)
		}
	}
	myStatuses := map[uuid.UUID]struct {
		id     string
		st     pb.ReportStatus
		reason string
	}{}
	if teamIDStr != "" {
		if tid, err := uuid.Parse(teamIDStr); err == nil {
			for _, incID := range allIncidentIDs {
				if rid, st, reason, err := s.repo.GetLatestReportMetaForTeam(ctx, incID, tid); err == nil {
					myStatuses[incID] = struct {
						id     string
						st     pb.ReportStatus
						reason string
					}{rid.String(), pb.ReportStatus(st), derefOr(reason, "")}
				}
			}
		}
	}
	acceptedList, err := s.repo.ListAcceptedRedReports(ctx, allIncidentIDs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "accepted red reports: %v", err)
	}
	acceptedMap := map[uuid.UUID]bool{}
	for _, ar := range acceptedList {
		acceptedMap[ar.IncidentID] = true
	}
	out := &pb.GetRedPolygonsResponse{}
	for _, p := range polys {
		pv := &pb.PolygonRedView{
			Id:          p.ID.String(),
			Name:        p.Name,
			Description: p.Description,
			CoverUrl:    p.CoverURL,
		}
		for _, in := range p.Incidents {
			iv := &pb.IncidentRedView{
				Id:          in.ID.String(),
				Name:        in.Name,
				Description: in.Description,
			}
			// Признак того, что уже есть принятый отчёт любой красной команды.
			if acceptedMap[in.ID] {
				iv.AlreadySolved = true
			}
			if in.BasePrize > 0 {
				iv.RedPrize = in.BasePrize
			}
			if in.BlueSharePercent > 0 {
				iv.BluePrizeProcent = int64(in.BlueSharePercent)
			}
			if ms, ok := myStatuses[in.ID]; ok {
				iv.MyReportStatus = ms.st
				iv.MyReportId = ms.id
				if ms.st == pb.ReportStatus_REPORT_STATUS_REJECTED {
					iv.MyRejectionReason = ms.reason
				}
			}
			pv.Incidents = append(pv.Incidents, iv)
		}
		out.Polygons = append(out.Polygons, pv)
	}
	return out, nil
}

func (s *PolygonServer) GetBluePolygon(ctx context.Context, _ *emptypb.Empty) (*pb.GetBluePolygonResponse, error) {
	_, teamIDStr, _ := s.extractAuth(ctx)
	if teamIDStr == "" {
		return &pb.GetBluePolygonResponse{}, nil
	}
	tid, err := uuid.Parse(teamIDStr)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid team id")
	}
	tm, err := s.repo.GetTeam(ctx, tid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "team: %v", err)
	}
	if tm.Type != int32(pb.TeamType_TEAM_TYPE_BLUE) {
		return &pb.GetBluePolygonResponse{}, nil
	}

	polID, err := s.repo.GetTeamPolygonID(ctx, tid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &pb.GetBluePolygonResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "team polygon: %v", err)
	}
	if polID == uuid.Nil {
		return &pb.GetBluePolygonResponse{}, nil
	}
	pol, err := s.repo.GetPolygon(ctx, polID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "polygon: %v", err)
	}
	incidents, err := s.repo.ListIncidents(ctx, polID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "incidents: %v", err)
	}
	incIDs := make([]uuid.UUID, 0, len(incidents))
	for _, in := range incidents {
		incIDs = append(incIDs, in.ID)
	}

	accepted, err := s.repo.ListAcceptedRedReports(ctx, incIDs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "accepted red: %v", err)
	}

	teamCache := map[uuid.UUID]*storage.Team{}
	getTeam := func(id uuid.UUID) *storage.Team {
		if v, ok := teamCache[id]; ok {
			return v
		}
		t, err2 := s.repo.GetTeam(ctx, id)
		if err2 != nil {
			return nil
		}
		teamCache[id] = t
		return t
	}

	myStatuses := map[uuid.UUID]struct {
		id     string
		st     pb.ReportStatus
		reason string
	}{}
	for _, inc := range incIDs {
		if rid, st, reason, err := s.repo.GetLatestReportMetaForTeam(ctx, inc, tid); err == nil {
			myStatuses[inc] = struct {
				id     string
				st     pb.ReportStatus
				reason string
			}{rid.String(), pb.ReportStatus(st), derefOr(reason, "")}
		}
	}
	var blueTeamPB *pb.Team
	if bt, err := s.repo.FindBlueTeamByPolygon(ctx, pol.ID); err == nil && bt != nil {
		blueTeamPB = &pb.Team{
			Id:   bt.ID.String(),
			Name: bt.Name,
			Type: pb.TeamType(bt.Type),
		}
	}
	pbPolygon := &pb.PolygonBlueView{
		Id:          pol.ID.String(),
		Name:        pol.Name,
		Description: pol.Description,
		CoverUrl:    pol.CoverURL,
		BlueTeam:    blueTeamPB,
	}

	for _, ar := range accepted {

		iv := &pb.IncidentBlueView{
			Id:                ar.IncidentID.String(),
			Name:              ar.IncidentName,
			Description:       ar.IncidentDescription,
			RedTeamReportId:   ar.ReportID.String(),
			RedTeamReportTime: uint32(ar.Time),
		}
		if ar.BasePrize > 0 {
			iv.RedPrize = ar.BasePrize
		}
		if ar.BlueSharePercent > 0 {
			iv.BluePrizeProcent = int64(ar.BlueSharePercent)
		}
		if tm := getTeam(ar.TeamID); tm != nil {
			iv.RedTeam = &pb.Team{
				Id:   tm.ID.String(),
				Name: tm.Name,
				Type: pb.TeamType(tm.Type),
			}
		}
		if ms, ok := myStatuses[ar.IncidentID]; ok {
			iv.MyReportStatus = ms.st
			iv.MyReportId = ms.id
			if ms.st == pb.ReportStatus_REPORT_STATUS_REJECTED {
				iv.MyRejectionReason = ms.reason
			}
		}
		pbPolygon.Incidents = append(pbPolygon.Incidents, iv)
	}
	return &pb.GetBluePolygonResponse{Polygon: pbPolygon}, nil
}

func (s *PolygonServer) GetRedIncidents(ctx context.Context, req *pb.GetRedIncidentsRequest) (*pb.GetRedIncidentsResponse, error) {
	if req.GetPolygonId() == "" {
		return nil, status.Error(codes.InvalidArgument, "polygon_id required")
	}
	pid, err := uuid.Parse(req.GetPolygonId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid polygon_id")
	}
	_, teamIDStr, _ := s.extractAuth(ctx)
	if teamIDStr == "" {
		return &pb.GetRedIncidentsResponse{}, nil
	}
	tid, err := uuid.Parse(teamIDStr)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid team id")
	}
	tm, err := s.repo.GetTeam(ctx, tid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "team: %v", err)
	}
	if tm.Type != int32(pb.TeamType_TEAM_TYPE_RED) {
		return &pb.GetRedIncidentsResponse{}, nil
	}
	incidents, err := s.repo.ListIncidents(ctx, pid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "incidents: %v", err)
	}
	// Build map of incidents having at least one accepted red report.
	incIDs := make([]uuid.UUID, 0, len(incidents))
	for _, in := range incidents {
		incIDs = append(incIDs, in.ID)
	}
	acceptedList, err := s.repo.ListAcceptedRedReports(ctx, incIDs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "accepted red reports: %v", err)
	}
	acceptedMap := map[uuid.UUID]bool{}
	for _, ar := range acceptedList {
		acceptedMap[ar.IncidentID] = true
	}

	out := &pb.GetRedIncidentsResponse{}
	for _, in := range incidents {
		iv := &pb.IncidentRedView{
			Id:          in.ID.String(),
			Name:        in.Name,
			Description: in.Description,
		}
		if acceptedMap[in.ID] {
			iv.AlreadySolved = true
		}
		if in.BasePrize > 0 {
			iv.RedPrize = in.BasePrize
		}
		if in.BlueSharePercent > 0 {
			iv.BluePrizeProcent = int64(in.BlueSharePercent)
		}
		if rid, st, reason, err := s.repo.GetLatestReportMetaForTeam(ctx, in.ID, tid); err == nil {
			iv.MyReportStatus = pb.ReportStatus(st)
			iv.MyReportId = rid.String()
			if iv.MyReportStatus == pb.ReportStatus_REPORT_STATUS_REJECTED {
				iv.MyRejectionReason = derefOr(reason, "")
			}
		}
		out.Incidents = append(out.Incidents, iv)
	}
	return out, nil
}

func (s *PolygonServer) GetBlueIncidents(ctx context.Context, _ *pb.GetBlueIncidentsRequest) (*pb.GetBlueIncidentsResponse, error) {
	_, teamIDStr, _ := s.extractAuth(ctx)
	if teamIDStr == "" {
		return &pb.GetBlueIncidentsResponse{}, nil
	}
	tid, err := uuid.Parse(teamIDStr)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid team id")
	}
	tm, err := s.repo.GetTeam(ctx, tid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "team: %v", err)
	}
	if tm.Type != int32(pb.TeamType_TEAM_TYPE_BLUE) {
		return &pb.GetBlueIncidentsResponse{}, nil
	}
	polID, err := s.repo.GetTeamPolygonID(ctx, tid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "team polygon: %v", err)
	}
	if polID == uuid.Nil {
		return &pb.GetBlueIncidentsResponse{}, nil
	}
	incidents, err := s.repo.ListIncidents(ctx, polID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "incidents: %v", err)
	}
	incIDs := make([]uuid.UUID, 0, len(incidents))
	for _, in := range incidents {
		incIDs = append(incIDs, in.ID)
	}
	accepted, err := s.repo.ListAcceptedRedReports(ctx, incIDs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "accepted red: %v", err)
	}

	myStatuses := map[uuid.UUID]struct {
		id     string
		st     pb.ReportStatus
		reason string
	}{}
	for _, inc := range incIDs {
		if rid, st, reason, err := s.repo.GetLatestReportMetaForTeam(ctx, inc, tid); err == nil {
			myStatuses[inc] = struct {
				id     string
				st     pb.ReportStatus
				reason string
			}{rid.String(), pb.ReportStatus(st), derefOr(reason, "")}
		}
	}
	teamCache := map[uuid.UUID]*storage.Team{}
	getTeam := func(id uuid.UUID) *storage.Team {
		if v, ok := teamCache[id]; ok {
			return v
		}
		t, err2 := s.repo.GetTeam(ctx, id)
		if err2 != nil {
			return nil
		}
		teamCache[id] = t
		return t
	}
	out := &pb.GetBlueIncidentsResponse{}
	for _, ar := range accepted {
		iv := &pb.IncidentBlueView{
			Id:                ar.IncidentID.String(),
			Name:              ar.IncidentName,
			Description:       ar.IncidentDescription,
			RedTeamReportId:   ar.ReportID.String(),
			RedTeamReportTime: uint32(ar.Time),
		}
		if ar.BasePrize > 0 {
			iv.RedPrize = ar.BasePrize
		}
		if ar.BlueSharePercent > 0 {
			iv.BluePrizeProcent = int64(ar.BlueSharePercent)
		}
		if tm := getTeam(ar.TeamID); tm != nil {
			iv.RedTeam = &pb.Team{
				Id:   tm.ID.String(),
				Name: tm.Name,
				Type: pb.TeamType(tm.Type),
			}
		}
		if ms, ok := myStatuses[ar.IncidentID]; ok {
			iv.MyReportStatus = ms.st
			iv.MyReportId = ms.id
			if ms.st == pb.ReportStatus_REPORT_STATUS_REJECTED {
				iv.MyRejectionReason = ms.reason
			}
		}
		out.Incidents = append(out.Incidents, iv)
	}
	return out, nil
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
	var blueTeamPB *pb.Team
	if bt := strings.TrimSpace(req.GetBlueTeamId()); bt != "" {
		tid, err := uuid.Parse(bt)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid blue_team_id")
		}
		tm, err := s.repo.GetTeam(ctx, tid)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "team: %v", err)
		}
		if tm.Type != int32(pb.TeamType_TEAM_TYPE_BLUE) {
			return nil, status.Error(codes.InvalidArgument, "team is not blue")
		}
		if err := s.repo.SetTeamPolygon(ctx, tid, id); err != nil {
			return nil, status.Errorf(codes.Internal, "set team polygon: %v", err)
		}
		blueTeamPB = &pb.Team{
			Id:   tm.ID.String(),
			Name: tm.Name,
			Type: pb.TeamType(tm.Type),
		}
	}
	return &pb.Polygon{
		Id:          id.String(),
		Name:        req.GetName(),
		Description: req.GetDescription(),
		CoverUrl:    req.GetCoverUrl(),
		BlueTeam:    blueTeamPB,
	}, nil
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
	var blueTeamPB *pb.Team
	if bt := strings.TrimSpace(req.GetBlueTeamId()); bt != "" {
		tid, err := uuid.Parse(bt)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid blue_team_id")
		}
		tm, err := s.repo.GetTeam(ctx, tid)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "team: %v", err)
		}
		if tm.Type != int32(pb.TeamType_TEAM_TYPE_BLUE) {
			return nil, status.Error(codes.InvalidArgument, "team is not blue")
		}
		if err := s.repo.SetTeamPolygon(ctx, tid, id); err != nil {
			return nil, status.Errorf(codes.Internal, "set team polygon: %v", err)
		}
		blueTeamPB = &pb.Team{
			Id:   tm.ID.String(),
			Name: tm.Name,
			Type: pb.TeamType(tm.Type),
		}
	}
	return &pb.Polygon{
		Id:          p.ID.String(),
		Name:        p.Name,
		Description: p.Description,
		CoverUrl:    p.CoverURL,
		BlueTeam:    blueTeamPB,
	}, nil
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

func (s *PolygonServer) ListPolygons(ctx context.Context, _ *emptypb.Empty) (*pb.AdminListPolygonsResponse, error) {
	polys, err := s.repo.ListPolygonsWithIncidents(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list polygons: %v", err)
	}
	resp := &pb.AdminListPolygonsResponse{}
	for _, p := range polys {
		var blueTeamPB *pb.Team
		if bt, err := s.repo.FindBlueTeamByPolygon(ctx, p.ID); err == nil && bt != nil {
			blueTeamPB = &pb.Team{
				Id:   bt.ID.String(),
				Name: bt.Name,
				Type: pb.TeamType(bt.Type),
			}
		}

		incIDs := make([]uuid.UUID, 0, len(p.Incidents))
		for _, in := range p.Incidents {
			incIDs = append(incIDs, in.ID)
		}
		redReportsByIncident, err := s.repo.ListReportsByIncidentsAndType(ctx, incIDs, int32(pb.TeamType_TEAM_TYPE_RED))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "load red reports: %v", err)
		}
		blueReportsByIncident, err := s.repo.ListReportsByIncidentsAndType(ctx, incIDs, int32(pb.TeamType_TEAM_TYPE_BLUE))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "load blue reports: %v", err)
		}

		var incs []*pb.Incident
		for _, in := range p.Incidents {
			toPBReports := func(list []storage.Report) []*pb.Report {
				out := make([]*pb.Report, 0, len(list))

				teamCache := map[uuid.UUID]*pb.Team{}
				for _, r := range list {
					if cached, ok := teamCache[r.TeamID]; ok {
						pbSteps := make([]*pb.ReportStep, 0, len(r.Steps))
						for _, s := range r.Steps {
							pbSteps = append(pbSteps, &pb.ReportStep{
								Id:          s.ID.String(),
								Number:      uint32(s.Number),
								Name:        s.Name,
								Time:        uint32(s.Time),
								Description: s.Description,
								Target:      s.Target,
								Source:      s.Source,
								Result:      s.Result,
							})
						}
						var redRef string
						if r.RedTeamReportID != nil {
							redRef = r.RedTeamReportID.String()
						}
						out = append(out, &pb.Report{
							Id:              r.ID.String(),
							IncidentId:      r.IncidentID.String(),
							IncidentName:    in.Name,
							Team:            cached,
							Steps:           pbSteps,
							Time:            uint32(r.Time),
							Status:          pb.ReportStatus(r.Status),
							RejectionReason: r.RejectionReason,
							RedTeamReportId: redRef,
						})
						continue
					}
					pr := s.toPBReport(ctx, &r)
					if pr != nil && pr.Team != nil {
						teamCache[r.TeamID] = pr.Team
					}
					out = append(out, pr)
				}
				return out
			}
			inc := &pb.Incident{
				Id:          in.ID.String(),
				Name:        in.Name,
				Description: in.Description,
			}
			if in.BasePrize > 0 {
				inc.RedPrize = in.BasePrize
			}
			if in.BlueSharePercent > 0 {
				inc.BluePrizeProcent = int64(in.BlueSharePercent)
			}
			if rr := redReportsByIncident[in.ID]; len(rr) > 0 {
				inc.RedReports = toPBReports(rr)
			}
			if br := blueReportsByIncident[in.ID]; len(br) > 0 {
				inc.BlueReports = toPBReports(br)
			}
			incs = append(incs, inc)
		}
		resp.Polygons = append(resp.Polygons, &pb.Polygon{
			Id:          p.ID.String(),
			Name:        p.Name,
			Description: p.Description,
			CoverUrl:    p.CoverURL,
			BlueTeam:    blueTeamPB,
			Incidents:   incs,
		})
	}
	return resp, nil
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
	return &pb.UploadPolygonCoverResponse{Cover: &pb.PolygonCoverMeta{
		Url:         url,
		ContentType: req.Cover.GetContentType(),
		Size:        size,
	}}, nil
}
