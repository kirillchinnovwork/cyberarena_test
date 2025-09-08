package main

import (
	"log"
	"os"

	"gis/polygon/services/polygon/internal/server"
)

func main() {
	addr := getEnv("POLYGON_GRPC_ADDR", ":50054")
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
