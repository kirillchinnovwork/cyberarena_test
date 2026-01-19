package server

import (
	"context"
	"encoding/json"
	"time"

	labv1 "gis/polygon/api/lab/v1"
	"gis/polygon/services/polygon/internal/storage"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	structpb "google.golang.org/protobuf/types/known/structpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

func (s *PolygonServer) GetLab(ctx context.Context, req *labv1.GetLabRequest) (*labv1.GetLabResponse, error) {
	if req.GetPolygonId() == "" {
		return nil, status.Error(codes.InvalidArgument, "polygon_id required")
	}

	polygonID, err := uuid.Parse(req.GetPolygonId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid polygon_id")
	}

	lab, err := s.repo.GetLabByPolygon(ctx, polygonID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "lab not found")
	}

	return &labv1.GetLabResponse{
		Lab: labToProto(lab),
	}, nil
}

func (s *PolygonServer) GetLabSteps(ctx context.Context, req *labv1.GetLabStepsRequest) (*labv1.GetLabStepsResponse, error) {
	if req.GetPolygonId() == "" {
		return nil, status.Error(codes.InvalidArgument, "polygon_id required")
	}

	polygonID, err := uuid.Parse(req.GetPolygonId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid polygon_id")
	}

	lab, err := s.repo.GetLabByPolygon(ctx, polygonID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "lab not found")
	}

	steps, err := s.repo.ListLabStepsPublic(ctx, lab.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list steps: %v", err)
	}

	protoSteps := make([]*labv1.LabStepPublic, len(steps))
	for i, step := range steps {
		protoSteps[i] = labStepToPublicProto(&step)
	}

	return &labv1.GetLabStepsResponse{
		Steps: protoSteps,
	}, nil
}

func (s *PolygonServer) GetStepAnswer(ctx context.Context, req *labv1.GetStepAnswerRequest) (*labv1.GetStepAnswerResponse, error) {
	if req.GetStepId() == "" {
		return nil, status.Error(codes.InvalidArgument, "step_id required")
	}

	stepID, err := uuid.Parse(req.GetStepId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid step_id")
	}

	step, err := s.repo.GetLabStep(ctx, stepID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "step not found")
	}

	return &labv1.GetStepAnswerResponse{
		Step: labStepToProto(step),
	}, nil
}

func (s *PolygonServer) ListLabs(ctx context.Context, req *labv1.ListLabsRequest) (*labv1.ListLabsResponse, error) {
	var polygonID *uuid.UUID
	if req.GetPolygonId() != "" {
		id, err := uuid.Parse(req.GetPolygonId())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid polygon_id")
		}
		polygonID = &id
	}

	labs, err := s.repo.ListLabs(ctx, polygonID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list labs: %v", err)
	}

	protoLabs := make([]*labv1.Lab, len(labs))
	for i, lab := range labs {
		protoLabs[i] = labToProto(&lab)
	}

	return &labv1.ListLabsResponse{
		Labs: protoLabs,
	}, nil
}

func (s *PolygonServer) CreateLab(ctx context.Context, req *labv1.CreateLabRequest) (*labv1.Lab, error) {
	if req.GetPolygonId() == "" || req.GetTitle() == "" {
		return nil, status.Error(codes.InvalidArgument, "polygon_id and title required")
	}

	polygonID, err := uuid.Parse(req.GetPolygonId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid polygon_id")
	}

	var groupID *uuid.UUID
	if req.GetGroupId() != "" {
		id, err := uuid.Parse(req.GetGroupId())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid group_id")
		}
		groupID = &id
	}

	var startedAt *time.Time
	if req.GetStartedAt() != nil {
		t := req.GetStartedAt().AsTime()
		startedAt = &t
	}

	lab := &storage.Lab{
		ID:          uuid.New(),
		PolygonID:   polygonID,
		Title:       req.GetTitle(),
		Description: req.GetDescription(),
		StartedAt:   startedAt,
		TTLSeconds:  req.GetTtlSeconds(),
		GroupID:     groupID,
		StepCount:   0,
		CreatedAt:   time.Now(),
	}

	if err := s.repo.CreateLab(ctx, lab); err != nil {
		return nil, status.Errorf(codes.Internal, "create lab: %v", err)
	}

	return labToProto(lab), nil
}

func (s *PolygonServer) UpdateLab(ctx context.Context, req *labv1.UpdateLabRequest) (*labv1.Lab, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}

	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}

	var title, description *string
	if req.GetTitle() != "" {
		title = &req.Title
	}
	if req.GetDescription() != "" {
		description = &req.Description
	}

	var startedAt *time.Time
	if req.GetStartedAt() != nil {
		t := req.GetStartedAt().AsTime()
		startedAt = &t
	}

	var ttlSeconds *int64
	if req.GetTtlSeconds() > 0 {
		ttlSeconds = &req.TtlSeconds
	}

	var groupID *uuid.UUID
	if req.GetGroupId() != "" {
		gid, err := uuid.Parse(req.GetGroupId())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid group_id")
		}
		groupID = &gid
	}

	if err := s.repo.UpdateLab(ctx, id, title, description, startedAt, ttlSeconds, groupID); err != nil {
		return nil, status.Errorf(codes.Internal, "update lab: %v", err)
	}

	lab, err := s.repo.GetLab(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get lab: %v", err)
	}

	return labToProto(lab), nil
}

func (s *PolygonServer) DeleteLab(ctx context.Context, req *labv1.DeleteLabRequest) (*emptypb.Empty, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}

	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}

	if err := s.repo.DeleteLab(ctx, id); err != nil {
		return nil, status.Errorf(codes.Internal, "delete lab: %v", err)
	}

	return &emptypb.Empty{}, nil
}

func (s *PolygonServer) ListLabStepsAdmin(ctx context.Context, req *labv1.ListLabStepsRequest) (*labv1.ListLabStepsResponse, error) {
	if req.GetLabId() == "" {
		return nil, status.Error(codes.InvalidArgument, "lab_id required")
	}

	labID, err := uuid.Parse(req.GetLabId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid lab_id")
	}

	steps, err := s.repo.ListLabSteps(ctx, labID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list steps: %v", err)
	}

	protoSteps := make([]*labv1.LabStep, len(steps))
	for i, step := range steps {
		protoSteps[i] = labStepToProto(&step)
	}

	return &labv1.ListLabStepsResponse{
		Steps: protoSteps,
	}, nil
}

func (s *PolygonServer) CreateLabStep(ctx context.Context, req *labv1.CreateLabStepRequest) (*labv1.LabStep, error) {
	if req.GetLabId() == "" || req.GetTitle() == "" {
		return nil, status.Error(codes.InvalidArgument, "lab_id and title required")
	}

	labID, err := uuid.Parse(req.GetLabId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid lab_id")
	}

	initialItems := []byte("{}")
	if req.GetInitialItems() != nil {
		initialItems, _ = json.Marshal(req.GetInitialItems().AsMap())
	}

	answer := []byte("{}")
	if req.GetAnswer() != nil {
		answer, _ = json.Marshal(req.GetAnswer().AsMap())
	}

	step := &storage.LabStep{
		ID:           uuid.New(),
		LabID:        labID,
		Title:        req.GetTitle(),
		Description:  req.GetDescription(),
		InitialItems: initialItems,
		HasAnswer:    req.GetHasAnswer(),
		Answer:       answer,
		OrderIndex:   req.GetOrderIndex(),
	}

	if err := s.repo.CreateLabStep(ctx, step); err != nil {
		return nil, status.Errorf(codes.Internal, "create step: %v", err)
	}

	return labStepToProto(step), nil
}

func (s *PolygonServer) UpdateLabStep(ctx context.Context, req *labv1.UpdateLabStepRequest) (*labv1.LabStep, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}

	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}

	var title, description *string
	if req.GetTitle() != "" {
		title = &req.Title
	}
	if req.GetDescription() != "" {
		description = &req.Description
	}

	var initialItems, answer *json.RawMessage
	if req.GetInitialItems() != nil {
		data, _ := json.Marshal(req.GetInitialItems().AsMap())
		raw := json.RawMessage(data)
		initialItems = &raw
	}
	if req.GetAnswer() != nil {
		data, _ := json.Marshal(req.GetAnswer().AsMap())
		raw := json.RawMessage(data)
		answer = &raw
	}

	var hasAnswer *bool
	if req.GetHasAnswer() {
		hasAnswer = &req.HasAnswer
	}

	var orderIndex *int32
	if req.GetOrderIndex() > 0 {
		orderIndex = &req.OrderIndex
	}

	if err := s.repo.UpdateLabStep(ctx, id, title, description, initialItems, answer, hasAnswer, orderIndex); err != nil {
		return nil, status.Errorf(codes.Internal, "update step: %v", err)
	}

	step, err := s.repo.GetLabStep(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get step: %v", err)
	}

	return labStepToProto(step), nil
}

func (s *PolygonServer) DeleteLabStep(ctx context.Context, req *labv1.DeleteLabStepRequest) (*emptypb.Empty, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}

	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}

	if err := s.repo.DeleteLabStep(ctx, id); err != nil {
		return nil, status.Errorf(codes.Internal, "delete step: %v", err)
	}

	return &emptypb.Empty{}, nil
}

func labToProto(lab *storage.Lab) *labv1.Lab {
	pb := &labv1.Lab{
		Id:          lab.ID.String(),
		PolygonId:   lab.PolygonID.String(),
		Title:       lab.Title,
		Description: lab.Description,
		TtlSeconds:  lab.TTLSeconds,
		StepCount:   lab.StepCount,
		CreatedAt:   timestamppb.New(lab.CreatedAt),
	}
	if lab.StartedAt != nil {
		pb.StartedAt = timestamppb.New(*lab.StartedAt)
	}
	if lab.GroupID != nil {
		pb.GroupId = lab.GroupID.String()
	}
	return pb
}

func labStepToProto(step *storage.LabStep) *labv1.LabStep {
	pb := &labv1.LabStep{
		Id:          step.ID.String(),
		LabId:       step.LabID.String(),
		Title:       step.Title,
		Description: step.Description,
		HasAnswer:   step.HasAnswer,
		OrderIndex:  step.OrderIndex,
	}

	if len(step.InitialItems) > 0 {
		var m map[string]interface{}
		if json.Unmarshal(step.InitialItems, &m) == nil {
			pb.InitialItems, _ = structpb.NewStruct(m)
		}
	}
	if len(step.Answer) > 0 {
		var m map[string]interface{}
		if json.Unmarshal(step.Answer, &m) == nil {
			pb.Answer, _ = structpb.NewStruct(m)
		}
	}

	return pb
}

func labStepToPublicProto(step *storage.LabStep) *labv1.LabStepPublic {
	pb := &labv1.LabStepPublic{
		Id:          step.ID.String(),
		LabId:       step.LabID.String(),
		Title:       step.Title,
		Description: step.Description,
		HasAnswer:   step.HasAnswer,
		OrderIndex:  step.OrderIndex,
	}

	if len(step.InitialItems) > 0 {
		var m map[string]interface{}
		if json.Unmarshal(step.InitialItems, &m) == nil {
			pb.InitialItems, _ = structpb.NewStruct(m)
		}
	}

	return pb
}
