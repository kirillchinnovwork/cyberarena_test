package server

import (
	"context"
	"errors"
	"net"
	"time"

	authv1 "gis/polygon/api/auth/v1"
	polygonv1 "gis/polygon/api/polygon/v1"
	usersv1 "gis/polygon/api/users/v1"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	authv1.UnimplementedAuthClientServiceServer
	authv1.UnimplementedAuthAdminServiceServer
	pool      *pgxpool.Pool
	users     usersv1.UsersAdminServiceClient
	polygon   polygonv1.PolygonClientServiceClient
	jwtSecret []byte
	jwtTTL    time.Duration
}

func New(pool *pgxpool.Pool, users usersv1.UsersAdminServiceClient, polygon polygonv1.PolygonClientServiceClient, secret []byte, ttl time.Duration) *Server {
	return &Server{pool: pool, users: users, polygon: polygon, jwtSecret: secret, jwtTTL: ttl}
}

func (s *Server) CreateUser(ctx context.Context, req *authv1.CreateUserRequest) (*authv1.CreateUserResponse, error) {
	if req.GetName() == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "name and password required")
	}
	user, err := s.users.CreateUser(ctx, &usersv1.CreateUserRequest{Name: req.GetName(), Avatar: req.GetAvatar()})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create user upstream: %v", err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.GetPassword()), bcrypt.DefaultCost)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "hash password: %v", err)
	}
	
	_, err = s.pool.Exec(ctx, `insert into auth_credentials (user_id, password_hash) values ($1,$2)
		ON CONFLICT (user_id) DO UPDATE SET password_hash = excluded.password_hash, updated_at = now()`, user.GetId(), string(hash))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "save password: %v", err)
	}
	return &authv1.CreateUserResponse{UserId: user.GetId(), Name: user.GetName(), AvatarUrl: user.GetAvatarUrl()}, nil
}

func (s *Server) SetPassword(ctx context.Context, req *authv1.SetPasswordRequest) (*emptypb.Empty, error) {
	if req.GetUserId() == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id and password required")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.GetPassword()), bcrypt.DefaultCost)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "hash password: %v", err)
	}
	ct, err := s.pool.Exec(ctx, `insert into auth_credentials (user_id, password_hash) values ($1,$2)
		ON CONFLICT (user_id) DO UPDATE SET password_hash = excluded.password_hash, updated_at = now()`, req.GetUserId(), string(hash))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "save password: %v", err)
	}
	if ct.RowsAffected() == 0 {
		return nil, status.Error(codes.Internal, "no rows affected")
	}
	return &emptypb.Empty{}, nil
}

type claimsWithTeam struct {
	TeamID string `json:"team_id,omitempty"`
	jwt.RegisteredClaims
}

func (s *Server) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	if req.GetName() == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "name and password required")
	}
	var stored string
	var userID string
	
	err := s.pool.QueryRow(ctx, `select c.user_id, c.password_hash from auth_credentials c join users u on u.id = c.user_id where u.name=$1`, req.GetName()).Scan(&userID, &stored)
	if err != nil {
		return nil, status.Error(codes.NotFound, "credentials not found")
	}
	if bcrypt.CompareHashAndPassword([]byte(stored), []byte(req.GetPassword())) != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	
	var teamID string
	if s.polygon != nil {
		ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := s.polygon.GetUserTeam(ctx2, &polygonv1.GetUserTeamRequest{UserId: userID})
		cancel()
		if err == nil && resp.GetTeam() != nil {
			teamID = resp.GetTeam().GetId()
		}
	}
	exp := time.Now().Add(s.jwtTTL)
	claims := claimsWithTeam{TeamID: teamID, RegisteredClaims: jwt.RegisteredClaims{Subject: userID, ExpiresAt: jwt.NewNumericDate(exp), IssuedAt: jwt.NewNumericDate(time.Now())}}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(s.jwtSecret)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "sign token: %v", err)
	}
	return &authv1.LoginResponse{AccessToken: signed, ExpiresAtUnix: exp.Unix(), UserId: userID, TeamId: teamID}, nil
}

func (s *Server) ValidateToken(ctx context.Context, req *authv1.ValidateTokenRequest) (*authv1.ValidateTokenResponse, error) {
	if req.GetAccessToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "token required")
	}
	parsed, err := jwt.ParseWithClaims(req.GetAccessToken(), &claimsWithTeam{}, func(token *jwt.Token) (interface{}, error) {
		if token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, errors.New("unexpected signing method")
		}
		return s.jwtSecret, nil
	})
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	cl, ok := parsed.Claims.(*claimsWithTeam)
	if !ok || !parsed.Valid || cl.Subject == "" {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	return &authv1.ValidateTokenResponse{UserId: cl.Subject, TeamId: cl.TeamID}, nil
}

func RunGRPC(addr string, srv *Server) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	g := grpc.NewServer()
	authv1.RegisterAuthClientServiceServer(g, srv)
	authv1.RegisterAuthAdminServiceServer(g, srv)
	return g.Serve(l)
}
