package server

import (
	"context"
	"errors"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	usersv1 "gis/polygon/api/users/v1"
	"gis/polygon/services/users/internal/media"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	httpbody "google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type UsersServer struct {
	usersv1.UnimplementedUsersClientServiceServer
	usersv1.UnimplementedUsersAdminServiceServer
	pool *pgxpool.Pool
	s3   *media.S3Storage
}

func (u *UsersServer) GetUser(ctx context.Context, request *usersv1.GetUserRequest) (*usersv1.User, error) {
	if request.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	row := u.pool.QueryRow(ctx, `select id, name, coalesce(avatar_url,'') from users where id=$1`, request.GetId())
	var id, name, avatarURL string
	if err := row.Scan(&id, &name, &avatarURL); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Errorf(codes.Internal, "select: %v", err)
	}
	return &usersv1.User{Id: id, Name: name, AvatarUrl: avatarURL}, nil
}

func (u *UsersServer) CreateUser(ctx context.Context, request *usersv1.CreateUserRequest) (*usersv1.User, error) {
	name := strings.TrimSpace(request.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	
	var dummy int
	err := u.pool.QueryRow(ctx, `select 1 from users where name=$1 limit 1`, name).Scan(&dummy)
	if err == nil {
		return nil, status.Error(codes.AlreadyExists, "name already taken")
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.Internal, "check duplicate: %v", err)
	}
	id := uuid.New().String()
	avatarURL := ""
	avatarKey := ""
	if len(request.GetAvatar()) > 0 && u.s3 != nil {
		ct := http.DetectContentType(request.GetAvatar())
		ext := extByContentType(ct)
		key := "avatars/" + id + ext
		url, _, err := u.s3.PutBytes(ctx, key, request.GetAvatar(), ct)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "store avatar: %v", err)
		}
		avatarURL = url
		avatarKey = key
	}
	_, err = u.pool.Exec(ctx, `insert into users(id, name, avatar_url, avatar_key) values ($1,$2,$3,$4)`, id, name, avatarURL, avatarKey)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, status.Error(codes.AlreadyExists, "name already taken")
		}
		return nil, status.Errorf(codes.Internal, "insert: %v", err)
	}
	return &usersv1.User{Id: id, Name: name, AvatarUrl: avatarURL}, nil
}

func (u *UsersServer) GetAllUsers(ctx context.Context, request *usersv1.GetAllUsersRequest) (*usersv1.GetAllUsersResponse, error) {
	page := request.GetPage()
	if page <= 0 {
		page = 1
	}
	pageSize := request.GetPageSize()
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize
	rows, err := u.pool.Query(ctx, `select id, name, coalesce(avatar_url,'') from users order by created_at desc limit $1 offset $2`, pageSize, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query: %v", err)
	}
	defer rows.Close()
	resp := &usersv1.GetAllUsersResponse{}
	for rows.Next() {
		var id, name, avatarURL string
		if err := rows.Scan(&id, &name, &avatarURL); err != nil {
			return nil, status.Errorf(codes.Internal, "scan: %v", err)
		}
		resp.Users = append(resp.Users, &usersv1.User{Id: id, Name: name, AvatarUrl: avatarURL})
	}
	if rows.Err() != nil {
		return nil, status.Errorf(codes.Internal, "rows: %v", rows.Err())
	}
	return resp, nil
}

func (u *UsersServer) EditUser(ctx context.Context, request *usersv1.EditUserRequest) (*usersv1.User, error) {
	if request.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	
	sets := []string{}
	args := []any{}
	idx := 1
	if request.Name != "" {
		newName := strings.TrimSpace(request.Name)
		if newName == "" {
			return nil, status.Error(codes.InvalidArgument, "name required")
		}
		
		var dummy int
		err := u.pool.QueryRow(ctx, `select 1 from users where name=$1 and id<>$2 limit 1`, newName, request.GetId()).Scan(&dummy)
		if err == nil {
			return nil, status.Error(codes.AlreadyExists, "name already taken")
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.Internal, "check duplicate: %v", err)
		}
		sets = append(sets, "name=$"+itoa(idx))
		args = append(args, newName)
		idx++
	}
	if len(request.Avatar) > 0 && u.s3 != nil {
		
		var oldKey string
		_ = u.pool.QueryRow(ctx, `select avatar_key from users where id=$1`, request.GetId()).Scan(&oldKey)
		ct := http.DetectContentType(request.Avatar)
		ext := extByContentType(ct)
		newKey := "avatars/" + request.GetId() + ext
		url, _, err := u.s3.PutBytes(ctx, newKey, request.Avatar, ct)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "store avatar: %v", err)
		}
		sets = append(sets, "avatar_url=$"+itoa(idx))
		args = append(args, url)
		idx++
		sets = append(sets, "avatar_key=$"+itoa(idx))
		args = append(args, newKey)
		idx++
		if oldKey != "" && oldKey != newKey {
			_ = u.s3.DeleteObject(ctx, oldKey)
		}
	}
	if len(sets) == 0 {
		return u.GetUser(ctx, &usersv1.GetUserRequest{Id: request.GetId()})
	}
	args = append(args, request.GetId())
	query := "update users set " + strings.Join(sets, ",") + ", updated_at=now() where id=$" + itoa(idx)
	ct, err := u.pool.Exec(ctx, query, args...)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, status.Error(codes.AlreadyExists, "name already taken")
		}
		return nil, status.Errorf(codes.Internal, "update: %v", err)
	}
	if ct.RowsAffected() == 0 {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	return u.GetUser(ctx, &usersv1.GetUserRequest{Id: request.GetId()})
}

func (u *UsersServer) GetUserAvatar(ctx context.Context, request *usersv1.GetUserRequest) (*httpbody.HttpBody, error) {
	if request.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	var key string
	if err := u.pool.QueryRow(ctx, `select avatar_key from users where id=$1`, request.GetId()).Scan(&key); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Errorf(codes.Internal, "select: %v", err)
	}
	if key == "" || u.s3 == nil {
		return nil, status.Error(codes.NotFound, "avatar not found")
	}
	obj, _, ct, err := u.s3.GetObject(ctx, key)
	if err != nil {
		return nil, status.Error(codes.Internal, "get avatar")
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, status.Error(codes.Internal, "read avatar")
	}
	return &httpbody.HttpBody{ContentType: ct, Data: data}, nil
}

func (u *UsersServer) GetCurrentUser(ctx context.Context, _ *emptypb.Empty) (*usersv1.User, error) {
	uid, err := extractUserID(ctx)
	if err != nil {
		return nil, err
	}
	return u.GetUser(ctx, &usersv1.GetUserRequest{Id: uid})
}

func extractUserID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "no metadata")
	}
	uid := firstNonEmpty(md.Get("x-user-id"))
	if uid == "" {
		return "", status.Error(codes.Unauthenticated, "no user id metadata")
	}
	return uid, nil
}

func firstNonEmpty(vals []string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func New(pool *pgxpool.Pool, s3 *media.S3Storage) *UsersServer {
	return &UsersServer{pool: pool, s3: s3}
}

func RunGRPC(addr string, srv *UsersServer) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	usersv1.RegisterUsersClientServiceServer(grpcServer, srv)
	usersv1.RegisterUsersAdminServiceServer(grpcServer, srv)
	log.Printf("users gRPC listening on %s", addr)
	return grpcServer.Serve(lis)
}

func InitSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `create table if not exists users (
		id uuid primary key,
		name text not null,
		avatar_url text,
		avatar_key text,
		created_at timestamptz not null default now(),
		updated_at timestamptz not null default now()
	);`)
	if err != nil {
		return err
	}
	_, _ = pool.Exec(ctx, `alter table users add column if not exists avatar_key text`)
	_, _ = pool.Exec(ctx, `create unique index if not exists idx_users_name on users (name)`)
	return nil
}

func itoa(i int) string { return strconv.FormatInt(int64(i), 10) }

func extByContentType(ct string) string {
	t, _, _ := mime.ParseMediaType(ct)
	switch t {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	default:
		return filepath.Ext(t)
	}
}
