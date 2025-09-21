package main

import (
	"context"
	"log"
	"os"
	"time"

	"gis/polygon/services/auth/internal/server"

	polygonv1 "gis/polygon/api/polygon/v1"
	usersv1 "gis/polygon/api/users/v1"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()

	grpcAddr := getEnv("AUTH_GRPC_ADDR", ":50053")
	pgDSN := getEnv("AUTH_PG_DSN", "postgres://postgres:postgres@postgres:5432/news?sslmode=disable")
	jwtSecret := getEnv("AUTH_JWT_SECRET", "dev-secret")
	jwtTTLStr := getEnv("AUTH_JWT_TTL", "1h")
	refreshTTLStr := getEnv("AUTH_REFRESH_TTL", "720h") // 30 дней по умолчанию
	usersAddr := getEnv("USERS_GRPC_ADDR", "users:50051")
	polygonAddr := getEnv("POLYGON_GRPC_ADDR", "polygon:50054")

	jwtTTL, err := time.ParseDuration(jwtTTLStr)
	if err != nil {
		log.Fatalf("parse AUTH_JWT_TTL: %v", err)
	}
	refreshTTL, err := time.ParseDuration(refreshTTLStr)
	if err != nil {
		log.Fatalf("parse AUTH_REFRESH_TTL: %v", err)
	}

	// Cookie config for refresh token
	cookieName := getEnv("AUTH_REFRESH_COOKIE_NAME", "refresh_token")
	cookieDomain := getEnv("AUTH_REFRESH_COOKIE_DOMAIN", "")
	cookieSecureStr := getEnv("AUTH_REFRESH_COOKIE_SECURE", "true")
	cookieSecure := true
	if cookieSecureStr == "false" || cookieSecureStr == "0" {
		cookieSecure = false
	}

	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	if err := server.InitSchema(ctx, pool); err != nil {
		log.Fatalf("init schema: %v", err)
	}

	conn, err := grpc.Dial(usersAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial users: %v", err)
	}
	defer conn.Close()
	usersAdminClient := usersv1.NewUsersAdminServiceClient(conn)

	polygonConn, err := grpc.Dial(polygonAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial polygon: %v", err)
	}
	defer polygonConn.Close()
	polygonClient := polygonv1.NewPolygonClientServiceClient(polygonConn)

	srv := server.New(pool, usersAdminClient, polygonClient, []byte(jwtSecret), jwtTTL, refreshTTL, cookieName, cookieDomain, cookieSecure)

	if err := server.RunGRPC(grpcAddr, srv); err != nil {
		log.Fatalf("auth grpc: %v", err)
	}
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
