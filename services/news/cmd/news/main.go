package main

import (
	"log"
	"os"

	"gis/polygon/services/news/internal/server"
)

func main() {
	addr := getEnv("NEWS_GRPC_ADDR", ":50052")
	if err := server.RunGRPC(addr); err != nil {
		log.Fatalf("news service failed: %v", err)
	}
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
