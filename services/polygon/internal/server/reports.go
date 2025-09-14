package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	pb "gis/polygon/api/polygon/v1"
	"gis/polygon/services/polygon/internal/storage"

	gatewayfile "github.com/black-06/grpc-gateway-file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	httpbody "google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *PolygonServer) SubmitReport(ctx context.Context, req *pb.SubmitReportRequest) (*pb.Report, error) {
	if req.GetIncidentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "incident_id required")
	}
	incidentID, err := uuid.Parse(req.GetIncidentId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid incident_id")
	}
	userID, teamID, err := s.extractAuth(ctx)
	if err != nil {
		return nil, err
	}
	if teamID == "" {
		return nil, status.Error(codes.PermissionDenied, "no team")
	}

	if _, err := s.repo.GetIncident(ctx, incidentID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "incident not found")
		}
		return nil, status.Errorf(codes.Internal, "incident: %v", err)
	}
	tid, _ := uuid.Parse(teamID)

	if exists, existingID, err := s.repo.ReportExistsForTeam(ctx, incidentID, tid); err == nil && exists {

		rp, err2 := s.repo.GetReport(ctx, existingID)
		if err2 == nil {
			return toPBReport(rp), nil
		}
	}
	reportID := uuid.New()
	steps := make([]storage.ReportStep, 0, len(req.GetSteps()))
	for i, st := range req.GetSteps() {
		steps = append(steps, storage.ReportStep{ID: uuid.New(), Number: int32(i + 1), Name: st.GetName(), Time: st.GetTime(), Description: st.GetDescription(), Target: st.GetTarget(), Source: st.GetSource(), Result: st.GetResult()})
	}
	if err := s.repo.InsertReport(ctx, reportID, incidentID, tid, int32(pb.ReportStatus_REPORT_STATUS_PENDING), storage.SumStepTime(steps)); err != nil {
		return nil, status.Errorf(codes.Internal, "insert report: %v", err)
	}
	if err := s.repo.InsertReportSteps(ctx, reportID, steps); err != nil {
		return nil, status.Errorf(codes.Internal, "insert steps: %v", err)
	}
	rp, err := s.repo.GetReport(ctx, reportID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reload: %v", err)
	}
	_ = userID
	return toPBReport(rp), nil
}
func (s *PolygonServer) UploadReportAttachment(stream pb.PolygonClientService_UploadReportAttachmentServer) error {
	formData, err := gatewayfile.NewFormData(stream, 50*1024*1024)
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
	key := s.s3.ObjectKey("report_attachments", "1", attID.String())
	_, size, err := s.s3.PutBytes(stream.Context(), key, buf.Bytes(), contentTypeOrDefault(fileHeader.Header.Get("Content-Type")))
	if err != nil {
		return status.Errorf(codes.Internal, "s3 put: %v", err)
	}

	return stream.SendAndClose(&pb.UploadReportAttachmentResponse{
		Attachment: &pb.ReportAttachment{
			Id:          attID.String(),
			Url:         "/v1/report/attachments/" + attID.String(),
			ContentType: contentTypeOrDefault(fileHeader.Header.Get("Content-Type")),
			Size:        size,
		},
	})
}
func (s *PolygonServer) DownloadReportAttachment(req *pb.DownloadReportAttachmentRequest, stream pb.PolygonClientService_DownloadReportAttachmentServer) error {
	if req.GetId() == "" {
		return status.Error(codes.InvalidArgument, "id required")
	}
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid id")
	}
	if s.s3 == nil {
		return status.Error(codes.FailedPrecondition, "s3 not configured")
	}
	key := s.s3.ObjectKey("report_attachments", "1", id.String())
	obj, _, ct, err := s.s3.GetObject(stream.Context(), key)
	if err != nil {
		return status.Error(codes.NotFound, "attachment not found")
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return status.Errorf(codes.Internal, "read: %v", err)
	}
	return stream.Send(&httpbody.HttpBody{ContentType: ct, Data: data})
}

func (s *PolygonServer) EditReport(ctx context.Context, req *pb.EditReportRequest) (*pb.Report, error) {
	if req.GetReportId() == "" {
		return nil, status.Error(codes.InvalidArgument, "report_id required")
	}
	reportID, err := uuid.Parse(req.GetReportId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid report_id")
	}
	_, teamID, err := s.extractAuth(ctx)
	if err != nil {
		return nil, err
	}
	rp, err := s.repo.GetReport(ctx, reportID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "report not found")
		}
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	if teamID == "" || rp.TeamID.String() != teamID {
		return nil, status.Error(codes.PermissionDenied, "forbidden")
	}
	if pb.ReportStatus(rp.Status) != pb.ReportStatus_REPORT_STATUS_REJECTED {
		return nil, status.Error(codes.FailedPrecondition, "only rejected can be edited")
	}
	steps := make([]storage.ReportStep, 0, len(req.GetSteps()))
	for i, st := range req.GetSteps() {
		steps = append(steps, storage.ReportStep{ID: uuid.New(), Number: int32(i + 1), Name: st.GetName(), Time: st.GetTime(), Description: st.GetDescription(), Target: st.GetTarget(), Source: st.GetSource(), Result: st.GetResult()})
	}
	if err := s.repo.ReplaceReportSteps(ctx, reportID, steps); err != nil {
		return nil, status.Errorf(codes.Internal, "replace: %v", err)
	}
	if err := s.repo.UpdateReportForEdit(ctx, reportID, int32(pb.ReportStatus_REPORT_STATUS_PENDING)); err != nil {
		return nil, status.Errorf(codes.Internal, "status: %v", err)
	}
	rp2, err := s.repo.GetReport(ctx, reportID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reload: %v", err)
	}
	return toPBReport(rp2), nil
}
func (s *PolygonServer) GetIncidentReport(ctx context.Context, req *pb.GetIncidentReportRequest) (*pb.Report, error) {
	if req.GetIncidentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "incident_id required")
	}
	incidentID, err := uuid.Parse(req.GetIncidentId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid incident_id")
	}
	_, teamID, err := s.extractAuth(ctx)
	if err != nil {
		return nil, err
	}
	if teamID == "" {
		return nil, status.Error(codes.PermissionDenied, "no team")
	}
	tid, _ := uuid.Parse(teamID)
	rp, err := s.repo.GetTeamIncidentReport(ctx, incidentID, tid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "report not found")
		}
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	// Преобразуем через обновлённый helper (incident_id поле)
	return toPBReport(rp), nil
}

func (s *PolygonServer) ReviewReport(ctx context.Context, req *pb.ReviewReportRequest) (*pb.Report, error) {
	if req.GetReportId() == "" {
		return nil, status.Error(codes.InvalidArgument, "report_id required")
	}
	if req.GetStatus() != pb.ReportStatus_REPORT_STATUS_ACCEPTED && req.GetStatus() != pb.ReportStatus_REPORT_STATUS_REJECTED {
		return nil, status.Error(codes.InvalidArgument, "status must be ACCEPTED or REJECTED")
	}
	if req.GetStatus() == pb.ReportStatus_REPORT_STATUS_REJECTED && strings.TrimSpace(req.GetReason()) == "" {
		return nil, status.Error(codes.InvalidArgument, "reason required for rejection")
	}
	reportID, err := uuid.Parse(req.GetReportId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid report_id")
	}
	var reasonPtr *string
	if req.GetStatus() == pb.ReportStatus_REPORT_STATUS_REJECTED {
		r := req.GetReason()
		reasonPtr = &r
	}
	if err := s.repo.UpdateReportStatus(ctx, reportID, int32(req.GetStatus()), reasonPtr); err != nil {
		return nil, status.Errorf(codes.Internal, "update: %v", err)
	}
	rp, err := s.repo.GetReport(ctx, reportID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return toPBReport(rp), nil
}
func (s *PolygonServer) GetTeamReports(ctx context.Context, req *pb.GetTeamReportsRequest) (*pb.GetTeamReportsResponse, error) {
	if req.GetTeamId() == "" {
		return nil, status.Error(codes.InvalidArgument, "team_id required")
	}
	teamID, err := uuid.Parse(req.GetTeamId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid team_id")
	}
	list, err := s.repo.ListTeamReports(ctx, teamID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	resp := &pb.GetTeamReportsResponse{}
	for _, rp := range list {
		resp.Reports = append(resp.Reports, toPBReport(&rp))
	}
	return resp, nil
}
