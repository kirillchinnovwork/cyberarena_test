package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	externalv1 "gis/polygon/api/external/v1"
	"gis/polygon/services/external_controller/internal/ansible"
	"gis/polygon/services/external_controller/internal/jenkins"
	"gis/polygon/services/external_controller/internal/terraform"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

type Server struct {
	externalv1.UnimplementedExternalControllerServiceServer

	jenkins   *jenkins.Client
	terraform *terraform.Client
	ansible   *ansible.Client

	jobs   map[string]*jobRecord
	jobsMu sync.RWMutex
}

type jobRecord struct {
	ID               string
	ExternalID       string
	Type             externalv1.JobType
	Status           externalv1.JobStatus
	Name             string
	Params           map[string]interface{}
	CreatedAt        time.Time
	StartedAt        *time.Time
	FinishedAt       *time.Time
	ErrorMessage     string
	JenkinsJobName   string
	JenkinsBuildNum  int
	TerraformRunID   string
	AnsibleProjectID int
	AnsibleTaskID    int
}

func NewServer(jenkinsClient *jenkins.Client, terraformClient *terraform.Client, ansibleClient *ansible.Client) *Server {
	return &Server{
		jenkins:   jenkinsClient,
		terraform: terraformClient,
		ansible:   ansibleClient,
		jobs:      make(map[string]*jobRecord),
	}
}

func (s *Server) RunJenkinsJob(ctx context.Context, req *externalv1.RunJenkinsJobRequest) (*externalv1.Job, error) {
	if s.jenkins == nil {
		return nil, status.Error(codes.Unavailable, "jenkins not configured")
	}
	if req.GetJobName() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_name required")
	}

	params := make(map[string]string)
	if req.GetParams() != nil {
		for k, v := range req.GetParams().AsMap() {
			params[k] = fmt.Sprintf("%v", v)
		}
	}

	queueID, err := s.jenkins.TriggerBuild(ctx, req.GetJobName(), params)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "trigger jenkins build: %v", err)
	}

	job := &jobRecord{
		ID:             uuid.New().String(),
		ExternalID:     fmt.Sprintf("%d", queueID),
		Type:           externalv1.JobType_JOB_TYPE_JENKINS,
		Status:         externalv1.JobStatus_JOB_STATUS_PENDING,
		Name:           req.GetJobName(),
		Params:         req.GetParams().AsMap(),
		CreatedAt:      time.Now(),
		JenkinsJobName: req.GetJobName(),
	}

	s.jobsMu.Lock()
	s.jobs[job.ID] = job
	s.jobsMu.Unlock()

	return jobToProto(job), nil
}

func (s *Server) RunTerraform(ctx context.Context, req *externalv1.RunTerraformRequest) (*externalv1.Job, error) {
	if s.terraform == nil {
		return nil, status.Error(codes.Unavailable, "terraform not configured")
	}
	if req.GetWorkspace() == "" {
		return nil, status.Error(codes.InvalidArgument, "workspace required")
	}

	isDestroy := req.GetAction() == "destroy"
	message := fmt.Sprintf("API triggered: %s", req.GetAction())

	vars := make(map[string]string)
	if req.GetVars() != nil {
		for k, v := range req.GetVars().AsMap() {
			vars[k] = fmt.Sprintf("%v", v)
		}
	}

	run, err := s.terraform.CreateRun(ctx, req.GetWorkspace(), message, isDestroy, vars)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create terraform run: %v", err)
	}

	now := time.Now()
	job := &jobRecord{
		ID:             uuid.New().String(),
		ExternalID:     run.ID,
		Type:           externalv1.JobType_JOB_TYPE_TERRAFORM,
		Status:         externalv1.JobStatus_JOB_STATUS_RUNNING,
		Name:           fmt.Sprintf("%s:%s", req.GetWorkspace(), req.GetAction()),
		Params:         req.GetVars().AsMap(),
		CreatedAt:      now,
		StartedAt:      &now,
		TerraformRunID: run.ID,
	}

	s.jobsMu.Lock()
	s.jobs[job.ID] = job
	s.jobsMu.Unlock()

	return jobToProto(job), nil
}

func (s *Server) RunAnsible(ctx context.Context, req *externalv1.RunAnsibleRequest) (*externalv1.Job, error) {
	if s.ansible == nil {
		return nil, status.Error(codes.Unavailable, "ansible/semaphore not configured")
	}
	if req.GetProjectId() == 0 || req.GetTemplateId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "project_id and template_id required")
	}

	extraVars := make(map[string]interface{})
	if req.GetExtraVars() != nil {
		extraVars = req.GetExtraVars().AsMap()
	}

	task, err := s.ansible.RunTask(ctx, int(req.GetProjectId()), int(req.GetTemplateId()), extraVars)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "run ansible task: %v", err)
	}

	now := time.Now()
	job := &jobRecord{
		ID:               uuid.New().String(),
		ExternalID:       strconv.Itoa(task.ID),
		Type:             externalv1.JobType_JOB_TYPE_ANSIBLE,
		Status:           externalv1.JobStatus_JOB_STATUS_RUNNING,
		Name:             fmt.Sprintf("project:%d/template:%d", req.GetProjectId(), req.GetTemplateId()),
		Params:           extraVars,
		CreatedAt:        now,
		StartedAt:        &now,
		AnsibleProjectID: int(req.GetProjectId()),
		AnsibleTaskID:    task.ID,
	}

	s.jobsMu.Lock()
	s.jobs[job.ID] = job
	s.jobsMu.Unlock()

	return jobToProto(job), nil
}

func (s *Server) GetJobStatus(ctx context.Context, req *externalv1.GetJobStatusRequest) (*externalv1.Job, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id required")
	}

	s.jobsMu.RLock()
	job, ok := s.jobs[req.GetJobId()]
	s.jobsMu.RUnlock()

	if !ok {
		return nil, status.Error(codes.NotFound, "job not found")
	}

	if err := s.refreshJobStatus(ctx, job); err != nil {
		fmt.Printf("refresh job status error: %v\n", err)
	}

	return jobToProto(job), nil
}

func (s *Server) GetJobLogs(ctx context.Context, req *externalv1.GetJobLogsRequest) (*externalv1.JobLog, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id required")
	}

	s.jobsMu.RLock()
	job, ok := s.jobs[req.GetJobId()]
	s.jobsMu.RUnlock()

	if !ok {
		return nil, status.Error(codes.NotFound, "job not found")
	}

	var content string
	var newOffset int64
	var moreAvailable bool

	switch job.Type {
	case externalv1.JobType_JOB_TYPE_JENKINS:
		if s.jenkins != nil && job.JenkinsBuildNum > 0 {
			var err error
			content, newOffset, moreAvailable, err = s.jenkins.GetBuildLog(ctx, job.JenkinsJobName, job.JenkinsBuildNum, req.GetOffset())
			if err != nil {
				return nil, status.Errorf(codes.Internal, "get jenkins logs: %v", err)
			}
		}

	case externalv1.JobType_JOB_TYPE_TERRAFORM:
		if s.terraform != nil && job.TerraformRunID != "" {
			var err error
			content, err = s.terraform.GetRunLogs(ctx, job.TerraformRunID)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "get terraform logs: %v", err)
			}
		}

	case externalv1.JobType_JOB_TYPE_ANSIBLE:
		if s.ansible != nil && job.AnsibleTaskID > 0 {
			outputs, err := s.ansible.GetTaskOutput(ctx, job.AnsibleProjectID, job.AnsibleTaskID)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "get ansible logs: %v", err)
			}
			for _, out := range outputs {
				content += fmt.Sprintf("[%s] %s\n%s\n", out.Time, out.Task, out.Output)
			}
		}
	}

	return &externalv1.JobLog{
		JobId:         job.ID,
		Content:       content,
		Offset:        newOffset,
		MoreAvailable: moreAvailable,
	}, nil
}

func (s *Server) CancelJob(ctx context.Context, req *externalv1.CancelJobRequest) (*emptypb.Empty, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id required")
	}

	s.jobsMu.RLock()
	job, ok := s.jobs[req.GetJobId()]
	s.jobsMu.RUnlock()

	if !ok {
		return nil, status.Error(codes.NotFound, "job not found")
	}

	switch job.Type {
	case externalv1.JobType_JOB_TYPE_JENKINS:
		if s.jenkins != nil && job.JenkinsBuildNum > 0 {
			if err := s.jenkins.StopBuild(ctx, job.JenkinsJobName, job.JenkinsBuildNum); err != nil {
				return nil, status.Errorf(codes.Internal, "stop jenkins build: %v", err)
			}
		}

	case externalv1.JobType_JOB_TYPE_TERRAFORM:
		if s.terraform != nil && job.TerraformRunID != "" {
			if err := s.terraform.CancelRun(ctx, job.TerraformRunID); err != nil {
				return nil, status.Errorf(codes.Internal, "cancel terraform run: %v", err)
			}
		}

	case externalv1.JobType_JOB_TYPE_ANSIBLE:
		if s.ansible != nil && job.AnsibleTaskID > 0 {
			if err := s.ansible.StopTask(ctx, job.AnsibleProjectID, job.AnsibleTaskID); err != nil {
				return nil, status.Errorf(codes.Internal, "stop ansible task: %v", err)
			}
		}
	}

	s.jobsMu.Lock()
	job.Status = externalv1.JobStatus_JOB_STATUS_CANCELLED
	now := time.Now()
	job.FinishedAt = &now
	s.jobsMu.Unlock()

	return &emptypb.Empty{}, nil
}

func (s *Server) ListJobs(ctx context.Context, req *externalv1.ListJobsRequest) (*externalv1.ListJobsResponse, error) {
	s.jobsMu.RLock()
	defer s.jobsMu.RUnlock()

	var jobs []*externalv1.Job
	for _, job := range s.jobs {
		if req.GetType() != externalv1.JobType_JOB_TYPE_UNSPECIFIED && job.Type != req.GetType() {
			continue
		}
		jobs = append(jobs, jobToProto(job))
	}

	total := len(jobs)
	offset := int(req.GetOffset())
	limit := int(req.GetLimit())
	if limit == 0 {
		limit = 50
	}

	if offset >= len(jobs) {
		jobs = nil
	} else {
		end := offset + limit
		if end > len(jobs) {
			end = len(jobs)
		}
		jobs = jobs[offset:end]
	}

	return &externalv1.ListJobsResponse{
		Jobs:  jobs,
		Total: int32(total),
	}, nil
}

func (s *Server) refreshJobStatus(ctx context.Context, job *jobRecord) error {
	switch job.Type {
	case externalv1.JobType_JOB_TYPE_JENKINS:
		if s.jenkins == nil {
			return nil
		}
		if job.JenkinsBuildNum == 0 {
			queueID, _ := strconv.ParseInt(job.ExternalID, 10, 64)
			queueItem, err := s.jenkins.GetQueueItem(ctx, queueID)
			if err != nil {
				return err
			}
			if queueItem.Executable != nil {
				job.JenkinsBuildNum = queueItem.Executable.Number
				now := time.Now()
				job.StartedAt = &now
				job.Status = externalv1.JobStatus_JOB_STATUS_RUNNING
			}
		}

		if job.JenkinsBuildNum > 0 {
			info, err := s.jenkins.GetBuildInfo(ctx, job.JenkinsJobName, job.JenkinsBuildNum)
			if err != nil {
				return err
			}
			if info.Building {
				job.Status = externalv1.JobStatus_JOB_STATUS_RUNNING
			} else {
				now := time.Now()
				job.FinishedAt = &now
				switch info.Result {
				case "SUCCESS":
					job.Status = externalv1.JobStatus_JOB_STATUS_SUCCESS
				case "FAILURE":
					job.Status = externalv1.JobStatus_JOB_STATUS_FAILED
				case "ABORTED":
					job.Status = externalv1.JobStatus_JOB_STATUS_CANCELLED
				}
			}
		}

	case externalv1.JobType_JOB_TYPE_TERRAFORM:
		if s.terraform == nil || job.TerraformRunID == "" {
			return nil
		}
		run, err := s.terraform.GetRun(ctx, job.TerraformRunID)
		if err != nil {
			return err
		}
		switch terraform.StatusToJobStatus(run.Status) {
		case "running":
			job.Status = externalv1.JobStatus_JOB_STATUS_RUNNING
		case "success":
			job.Status = externalv1.JobStatus_JOB_STATUS_SUCCESS
			now := time.Now()
			job.FinishedAt = &now
		case "failed":
			job.Status = externalv1.JobStatus_JOB_STATUS_FAILED
			now := time.Now()
			job.FinishedAt = &now
		case "cancelled":
			job.Status = externalv1.JobStatus_JOB_STATUS_CANCELLED
			now := time.Now()
			job.FinishedAt = &now
		}

	case externalv1.JobType_JOB_TYPE_ANSIBLE:
		if s.ansible == nil || job.AnsibleTaskID == 0 {
			return nil
		}
		task, err := s.ansible.GetTask(ctx, job.AnsibleProjectID, job.AnsibleTaskID)
		if err != nil {
			return err
		}
		switch ansible.StatusToJobStatus(task.Status) {
		case "running":
			job.Status = externalv1.JobStatus_JOB_STATUS_RUNNING
		case "success":
			job.Status = externalv1.JobStatus_JOB_STATUS_SUCCESS
			now := time.Now()
			job.FinishedAt = &now
		case "failed":
			job.Status = externalv1.JobStatus_JOB_STATUS_FAILED
			job.ErrorMessage = task.Message
			now := time.Now()
			job.FinishedAt = &now
		case "cancelled":
			job.Status = externalv1.JobStatus_JOB_STATUS_CANCELLED
			now := time.Now()
			job.FinishedAt = &now
		}
	}

	return nil
}

func jobToProto(job *jobRecord) *externalv1.Job {
	pb := &externalv1.Job{
		Id:           job.ID,
		ExternalId:   job.ExternalID,
		Type:         job.Type,
		Status:       job.Status,
		Name:         job.Name,
		CreatedAt:    job.CreatedAt.Format(time.RFC3339),
		ErrorMessage: job.ErrorMessage,
	}

	if job.Params != nil {
		pb.Params, _ = structpb.NewStruct(job.Params)
	}
	if job.StartedAt != nil {
		pb.StartedAt = job.StartedAt.Format(time.RFC3339)
	}
	if job.FinishedAt != nil {
		pb.FinishedAt = job.FinishedAt.Format(time.RFC3339)
	}

	return pb
}

func RunGRPC(addr string, srv *Server) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	g := grpc.NewServer()
	externalv1.RegisterExternalControllerServiceServer(g, srv)
	return g.Serve(l)
}

func MarshalParams(params map[string]interface{}) string {
	if params == nil {
		return "{}"
	}
	data, _ := json.Marshal(params)
	return string(data)
}
