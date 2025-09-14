package main

import (
	"log"
	"os"

	"gis/polygon/services/attachments/internal/server"
)

func main() {
	addr := os.Getenv("ATTACHMENTS_GRPC_ADDR")
	if addr == "" {
		addr = ":50058"
	}
	if err := server.RunGRPC(addr); err != nil {
		log.Fatalf("run: %v", err)
	}
}
