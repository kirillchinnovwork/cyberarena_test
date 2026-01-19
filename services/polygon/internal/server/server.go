package server

import (
	"context"
	"log"
	"net"
	"os"
	"strings"

	pb "gis/polygon/api/polygon/v1"
	upb "gis/polygon/api/users/v1"
	"gis/polygon/services/polygon/internal/media"
	"gis/polygon/services/polygon/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type PolygonServer struct {
	pb.UnimplementedPolygonClientServiceServer
	pb.UnimplementedPolygonAdminServiceServer
	repo             *storage.Repo
	s3               *media.S3Storage
	jwtSecret        []byte
	usersClient      upb.UsersClientServiceClient
	usersAdminClient upb.UsersAdminServiceClient
}

func RunGRPC(addr string) error {
	pgDsn := getenv("POLYGON_PG_DSN", "postgres://postgres:postgres@localhost:5432/cyberarena?sslmode=disable")
	pool, err := pgxpool.New(context.Background(), pgDsn)
	if err != nil {
		return err
	}
	repo := storage.NewRepo(pool)
	if err := repo.Migrate(context.Background()); err != nil {
		return err
	}
	if err := repo.MigrateLabs(context.Background()); err != nil {
		log.Printf("labs migration error: %v", err)
	}
	jwtSecret := []byte(getenv("POLYGON_JWT_SECRET", getenv("AUTH_JWT_SECRET", "dev-secret")))
	s3Endpoint := getenv("POLYGON_S3_ENDPOINT", "localhost:9000")
	s3Access := getenv("POLYGON_S3_ACCESS_KEY", "minioadmin")
	s3Secret := getenv("POLYGON_S3_SECRET_KEY", "minioadmin")
	s3Bucket := getenv("POLYGON_S3_BUCKET", "polygon")
	useSSL := getenv("POLYGON_S3_USE_SSL", "false") == "true"
	publicBase := getenv("POLYGON_S3_PUBLIC_BASE", "")
	s3, err := media.NewS3(context.Background(), s3Endpoint, s3Access, s3Secret, s3Bucket, useSSL, publicBase)
	if err != nil {
		log.Printf("s3 init error: %v (continuing without s3)", err)
		s3 = nil
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	usersAddr := getenv("USERS_GRPC_ADDR", "")
	var usersCl upb.UsersClientServiceClient
	var usersAdm upb.UsersAdminServiceClient
	if usersAddr != "" {
		conn, err := grpc.Dial(usersAddr, grpc.WithInsecure())
		if err != nil {
			log.Printf("users dial failed: %v", err)
		} else {
			usersCl = upb.NewUsersClientServiceClient(conn)
			usersAdm = upb.NewUsersAdminServiceClient(conn)
		}
	}
	srv := &PolygonServer{repo: repo, s3: s3, jwtSecret: jwtSecret, usersClient: usersCl, usersAdminClient: usersAdm}
	pb.RegisterPolygonClientServiceServer(grpcServer, srv)
	pb.RegisterPolygonAdminServiceServer(grpcServer, srv)
	log.Printf("polygon gRPC listening on %s", addr)
	return grpcServer.Serve(lis)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func (s *PolygonServer) extractAuth(ctx context.Context) (string, string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", status.Error(codes.Unauthenticated, "no metadata")
	}
	uid := firstNonEmpty(md.Get("x-user-id"))
	team := firstNonEmpty(md.Get("x-team-id"))
	if uid == "" {
		return "", "", status.Error(codes.Unauthenticated, "no user id metadata")
	}
	return uid, team, nil
}

func firstNonEmpty(vals []string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func (s *PolygonServer) toPBReport(ctx context.Context, r *storage.Report) *pb.Report {
	if r == nil {
		return nil
	}
	pbSteps := make([]*pb.ReportStep, 0, len(r.Steps))
	for _, s := range r.Steps {
		pbSteps = append(pbSteps, &pb.ReportStep{Id: s.ID.String(), Number: uint32(s.Number), Name: s.Name, Time: uint32(s.Time), Description: s.Description, Target: s.Target, Source: s.Source, Result: s.Result})
	}
	var redRef string
	if r.RedTeamReportID != nil {
		redRef = r.RedTeamReportID.String()
	}
	teamPB := &pb.Team{Id: r.TeamID.String()}
	if t, err := s.repo.GetTeam(ctx, r.TeamID); err == nil && t != nil {
		teamPB.Name = t.Name
		teamPB.Type = pb.TeamType(t.Type)
		if userIDs, err2 := s.repo.ListTeamUserIDs(ctx, t.ID); err2 == nil && len(userIDs) > 0 {
			if s.usersClient != nil {
				for _, uid := range userIDs {
					if uResp, err3 := s.usersClient.GetUser(ctx, &upb.GetUserRequest{Id: uid.String()}); err3 == nil && uResp != nil {
						teamPB.Users = append(teamPB.Users, uResp)
					} else {
						teamPB.Users = append(teamPB.Users, &upb.User{Id: uid.String()})
					}
				}
			} else {
				for _, uid := range userIDs {
					teamPB.Users = append(teamPB.Users, &upb.User{Id: uid.String()})
				}
			}
		}
	}
	var incidentName, polygonName string
	if in, err := s.repo.GetIncident(ctx, r.IncidentID); err == nil && in != nil {
		incidentName = in.Name
	}
	if pn, err := s.repo.GetIncidentPolygonName(ctx, r.IncidentID); err == nil {
		polygonName = pn
	}
	return &pb.Report{Id: r.ID.String(), IncidentId: r.IncidentID.String(), IncidentName: incidentName, PolygonName: polygonName, Team: teamPB, Steps: pbSteps, Time: uint32(r.Time), Status: pb.ReportStatus(r.Status), RejectionReason: r.RejectionReason, RedTeamReportId: redRef}
}

func derefOr(p *string, def string) string {
	if p != nil {
		return *p
	}
	return def
}

func contentTypeOrDefault(ct string) string {
	if strings.TrimSpace(ct) == "" {
		return "application/octet-stream"
	}
	return ct
}
