package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"gis/polygon/services/users/internal/media"
	"gis/polygon/services/users/internal/server"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	addr := getEnv("USERS_GRPC_ADDR", ":50051")
	pgDSN := getEnv("USERS_PG_DSN", "postgres://postgres:postgres@postgres:5432/news?sslmode=disable")

	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	ctxPing, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for ctxPing.Err() == nil {
		if err := pool.Ping(ctxPing); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if err := server.InitSchema(ctx, pool); err != nil {
		log.Fatalf("init schema: %v", err)
	}

	var s3Store *media.S3Storage
	endpoint := os.Getenv("USERS_S3_ENDPOINT")
	bucket := getEnv("USERS_S3_BUCKET", "users")
	if endpoint != "" {
		access := getEnv("USERS_S3_ACCESS_KEY", "minioadmin")
		secret := getEnv("USERS_S3_SECRET_KEY", "minioadmin")
		pub := os.Getenv("USERS_S3_PUBLIC_BASE")
		useSSL, _ := strconv.ParseBool(getEnv("USERS_S3_USE_SSL", "false"))
		store, err := media.NewS3(ctx, endpoint, access, secret, bucket, useSSL, pub)
		if err != nil {
			log.Fatalf("init s3: %v", err)
		}
		s3Store = store
	}

	srv := server.New(pool, s3Store)
	if err := server.RunGRPC(addr, srv); err != nil {
		log.Fatalf("users service failed: %v", err)
	}
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
