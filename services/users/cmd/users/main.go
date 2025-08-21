package main

import (
	"log"
	"os"

	"gis/polygon/services/users/internal/server"
)

func main() {
	addr := getEnv("USERS_GRPC_ADDR", ":50051")
	if err := server.RunGRPC(addr); err != nil {
		log.Fatalf("users service failed: %v", err)
	}
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
