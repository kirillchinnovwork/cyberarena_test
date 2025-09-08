package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	authv1 "gis/polygon/api/auth/v1"
	newsv1 "gis/polygon/api/news/v1"
	polygonv1 "gis/polygon/api/polygon/v1"
	usersv1 "gis/polygon/api/users/v1"

	"github.com/black-06/grpc-gateway-file"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rs/cors"
)


func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := runtime.NewServeMux(
		gatewayfile.WithFileIncomingHeaderMatcher(),
		gatewayfile.WithFileForwardResponseOption(),
		gatewayfile.WithHTTPBodyMarshaler(),
	)

	usersAddr := getEnv("USERS_GRPC_ADDR", "users:50051")
	newsAddr := getEnv("NEWS_GRPC_ADDR", "news:50052")
	authAddr := getEnv("AUTH_GRPC_ADDR", "auth:50053")
	polygonAddr := getEnv("POLYGON_GRPC_ADDR", "polygon:50054")

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	if err := usersv1.RegisterUsersAdminServiceHandlerFromEndpoint(ctx, mux, usersAddr, dialOpts); err != nil {
		log.Fatalf("register users admin handler: %v", err)
	}
	if err := newsv1.RegisterNewsAdminServiceHandlerFromEndpoint(ctx, mux, newsAddr, dialOpts); err != nil {
		log.Fatalf("register news handler: %v", err)
	}
	if err := polygonv1.RegisterPolygonAdminServiceHandlerFromEndpoint(ctx, mux, polygonAddr, dialOpts); err != nil {
		log.Fatalf("register polygon handler: %v", err)
	}
	if err := authv1.RegisterAuthAdminServiceHandlerFromEndpoint(ctx, mux, authAddr, dialOpts); err != nil {
		log.Fatalf("register auth admin handler: %v", err)
	}

	httpAddr := getEnv("GATEWAY_HTTP_ADDR", ":8080")
	srv := &http.Server{Addr: httpAddr, Handler: cors.AllowAll().Handler(mux), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("gateway HTTP listening on %s", httpAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gateway failed: %v", err)
	}
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
