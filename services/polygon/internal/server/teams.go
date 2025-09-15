package server

import (
	"context"
	"errors"
	"log"
	"strings"

	pb "gis/polygon/api/polygon/v1"
	upb "gis/polygon/api/users/v1"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *PolygonServer) GetTeams(ctx context.Context, _ *emptypb.Empty) (*pb.GetTeamsResponse, error) {
	list, err := s.repo.ListTeamsWithUsers(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list teams: %v", err)
	}
	prizes, err := s.repo.ListTeamPrizes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "team prizes: %v", err)
	}
	resp := &pb.GetTeamsResponse{}

	if s.usersAdminClient == nil {
		userCache := map[string]*upb.User{}
		for _, t := range list {
			pbTeam := &pb.Team{Id: t.ID.String(), Name: t.Name, Type: pb.TeamType(t.Type)}
			if v, ok := prizes[t.ID]; ok {
				pbTeam.PrizeTotal = v
			}
			for _, uid := range t.UserIDs {
				uidStr := uid.String()
				if s.usersClient != nil {
					if u, ok := userCache[uidStr]; ok {
						pbTeam.Users = append(pbTeam.Users, u)
						continue
					}
					if uResp, err2 := s.usersClient.GetUser(ctx, &upb.GetUserRequest{Id: uidStr}); err2 == nil && uResp != nil {
						userCache[uidStr] = uResp
						pbTeam.Users = append(pbTeam.Users, uResp)
						continue
					}
				}
				pbTeam.Users = append(pbTeam.Users, &upb.User{Id: uidStr})
			}
			resp.Teams = append(resp.Teams, pbTeam)
		}
		return resp, nil
	}

	userCache := make(map[string]*upb.User)
	page := int32(1)
	pageSize := int32(500)
	for {
		res, err := s.usersAdminClient.GetAllUsers(ctx, &upb.GetAllUsersRequest{Page: page, PageSize: pageSize})
		if err != nil {
			log.Printf("users GetAllUsers page %d error: %v", page, err)
			break
		}
		for _, u := range res.GetUsers() {
			userCache[u.GetId()] = u
		}
		if len(res.GetUsers()) < int(pageSize) {
			break
		}
		page++
		if page > 10000 {
			break
		}
	}
	for _, t := range list {
		pbTeam := &pb.Team{Id: t.ID.String(), Name: t.Name, Type: pb.TeamType(t.Type), Users: []*upb.User{}}
		if v, ok := prizes[t.ID]; ok {
			pbTeam.PrizeTotal = v
		}
		for _, uid := range t.UserIDs {
			if u, ok := userCache[uid.String()]; ok {
				pbTeam.Users = append(pbTeam.Users, &upb.User{Id: u.GetId(), Name: u.GetName(), AvatarUrl: u.GetAvatarUrl()})
			} else {
				pbTeam.Users = append(pbTeam.Users, &upb.User{Id: uid.String()})
			}
		}
		resp.Teams = append(resp.Teams, pbTeam)
	}
	return resp, nil
}

func (s *PolygonServer) CreateTeam(ctx context.Context, req *pb.CreateTeamRequest) (*pb.Team, error) {
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	id := uuid.New()
	if err := s.repo.CreateTeam(ctx, id, name, int32(req.GetType())); err != nil {
		return nil, status.Errorf(codes.Internal, "create: %v", err)
	}
	return &pb.Team{Id: id.String(), Name: name, Type: req.GetType()}, nil
}
func (s *PolygonServer) EditTeam(ctx context.Context, req *pb.EditTeamRequest) (*pb.Team, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	var tptr *int32
	if req.Type != pb.TeamType(0) || req.Name == "" {
		v := int32(req.GetType())
		tptr = &v
	}
	if err := s.repo.UpdateTeam(ctx, id, strings.TrimSpace(req.GetName()), tptr); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "team not found")
		}
		return nil, status.Errorf(codes.Internal, "update: %v", err)
	}
	st, err := s.repo.GetTeam(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return &pb.Team{Id: st.ID.String(), Name: st.Name, Type: pb.TeamType(st.Type)}, nil
}
func (s *PolygonServer) DeleteTeam(ctx context.Context, req *pb.DeleteTeamRequest) (*emptypb.Empty, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid id")
	}
	if err := s.repo.DeleteTeam(ctx, id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "team not found")
		}
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &emptypb.Empty{}, nil
}
func (s *PolygonServer) AddUserToTeam(ctx context.Context, req *pb.AddUserToTeamRequest) (*emptypb.Empty, error) {
	if req.GetTeamId() == "" || req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "team_id and user_id required")
	}
	tid, err := uuid.Parse(req.GetTeamId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid team_id")
	}
	uid, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}
	if err := s.repo.AddUserToTeam(ctx, tid, uid); err != nil {
		return nil, status.Errorf(codes.Internal, "add: %v", err)
	}
	return &emptypb.Empty{}, nil
}
func (s *PolygonServer) RemoveUserFromTeam(ctx context.Context, req *pb.RemoveUserFromTeamRequest) (*emptypb.Empty, error) {
	if req.GetTeamId() == "" || req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "team_id and user_id required")
	}
	tid, err := uuid.Parse(req.GetTeamId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid team_id")
	}
	uid, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}
	if err := s.repo.RemoveUserFromTeam(ctx, tid, uid); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "relation not found")
		}
		return nil, status.Errorf(codes.Internal, "remove: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *PolygonServer) GetUserTeam(ctx context.Context, req *pb.GetUserTeamRequest) (*pb.GetUserTeamResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id required")
	}
	uid, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}
	team, err := s.repo.GetUserTeam(ctx, uid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &pb.GetUserTeamResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return &pb.GetUserTeamResponse{Team: &pb.Team{Id: team.ID.String(), Name: team.Name, Type: pb.TeamType(team.Type)}}, nil
}
