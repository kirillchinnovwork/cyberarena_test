package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	attv1 "gis/polygon/api/attachments/v1"
	authv1 "gis/polygon/api/auth/v1"
	newsv1 "gis/polygon/api/news/v1"
	polygonv1 "gis/polygon/api/polygon/v1"
	usersv1 "gis/polygon/api/users/v1"

	"gis/polygon/services/gateway/internal/middleware"

	gatewayfile "github.com/black-06/grpc-gateway-file"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/cors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	usersAddr := getEnv("USERS_GRPC_ADDR", "users:50051")
	newsAddr := getEnv("NEWS_GRPC_ADDR", "news:50052")
	authAddr := getEnv("AUTH_GRPC_ADDR", "auth:50053")
	polygonAddr := getEnv("POLYGON_GRPC_ADDR", "polygon:50054")
	attachmentsAddr := getEnv("ATTACHMENTS_GRPC_ADDR", "attachments:50055")
	externalControllerAddr := getEnv("EXTERNAL_CONTROLLER_GRPC_ADDR", "external_controller:50056")
	jwtSecret := getEnv("JWT_SECRET", "dev-secret")
	refreshCookieName := getEnv("AUTH_REFRESH_COOKIE_NAME", "refresh_token")

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	mux := runtime.NewServeMux(
		gatewayfile.WithFileIncomingHeaderMatcher(),
		gatewayfile.WithFileForwardResponseOption(),
		gatewayfile.WithHTTPBodyMarshaler(),

		runtime.WithMetadata(func(c context.Context, r *http.Request) metadata.MD {
			md := metadata.MD{}

			if uid := middleware.GetUserID(r.Context()); uid != "" {
				md.Append("x-user-id", uid)
			}
			if tid := middleware.GetTeamID(r.Context()); tid != "" {
				md.Append("x-team-id", tid)
			}
			if role := middleware.GetRole(r.Context()); role != "" {
				md.Append("x-user-role", string(role))
			}

			if authz := r.Header.Get("Authorization"); authz != "" {
				md.Append("authorization", authz)
			}

			if c, err := r.Cookie(refreshCookieName); err == nil && c != nil && c.Value != "" {
				md.Append("x-refresh-token", c.Value)
			}

			if len(md) == 0 {
				return nil
			}
			return md
		}),

		runtime.WithForwardResponseOption(func(ctx context.Context, w http.ResponseWriter, _ proto.Message) error {
			if sm, ok := runtime.ServerMetadataFromContext(ctx); ok {
				if cookies := sm.HeaderMD.Get("set-cookie"); len(cookies) > 0 {
					for _, c := range cookies {
						w.Header().Add("Set-Cookie", c)
					}
				}
			}
			return nil
		}),
	)

	if err := usersv1.RegisterUsersClientServiceHandlerFromEndpoint(ctx, mux, usersAddr, dialOpts); err != nil {
		log.Fatalf("register users client handler: %v", err)
	}
	if err := newsv1.RegisterNewsClientServiceHandlerFromEndpoint(ctx, mux, newsAddr, dialOpts); err != nil {
		log.Fatalf("register news client handler: %v", err)
	}
	if err := polygonv1.RegisterPolygonClientServiceHandlerFromEndpoint(ctx, mux, polygonAddr, dialOpts); err != nil {
		log.Fatalf("register polygon client handler: %v", err)
	}
	if err := authv1.RegisterAuthClientServiceHandlerFromEndpoint(ctx, mux, authAddr, dialOpts); err != nil {
		log.Fatalf("register auth client handler: %v", err)
	}
	if err := attv1.RegisterAttachmentsClientServiceHandlerFromEndpoint(ctx, mux, attachmentsAddr, dialOpts); err != nil {
		log.Fatalf("register attachments client handler: %v", err)
	}

	if err := usersv1.RegisterUsersAdminServiceHandlerFromEndpoint(ctx, mux, usersAddr, dialOpts); err != nil {
		log.Fatalf("register users admin handler: %v", err)
	}
	if err := newsv1.RegisterNewsAdminServiceHandlerFromEndpoint(ctx, mux, newsAddr, dialOpts); err != nil {
		log.Fatalf("register news admin handler: %v", err)
	}
	if err := polygonv1.RegisterPolygonAdminServiceHandlerFromEndpoint(ctx, mux, polygonAddr, dialOpts); err != nil {
		log.Fatalf("register polygon admin handler: %v", err)
	}
	if err := authv1.RegisterAuthAdminServiceHandlerFromEndpoint(ctx, mux, authAddr, dialOpts); err != nil {
		log.Fatalf("register auth admin handler: %v", err)
	}
	if err := attv1.RegisterAttachmentsAdminServiceHandlerFromEndpoint(ctx, mux, attachmentsAddr, dialOpts); err != nil {
		log.Fatalf("register attachments admin handler: %v", err)
	}

	_ = registerExternalController(ctx, mux, externalControllerAddr, dialOpts)

	authMiddleware := middleware.NewAuthMiddleware([]byte(jwtSecret))

	handler := authMiddleware.Handler(mux)

	handler = configureCORS(handler)

	httpAddr := getEnv("GATEWAY_HTTP_ADDR", ":8080")
	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("unified gateway HTTP listening on %s", httpAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gateway failed: %v", err)
	}
}

func registerExternalController(ctx context.Context, mux *runtime.ServeMux, addr string, opts []grpc.DialOption) error {
	_ = ctx
	_ = mux
	_ = addr
	_ = opts
	return nil
}

func configureCORS(handler http.Handler) http.Handler {
	originsCsv := getEnv("GATEWAY_CORS_ORIGINS", "")
	allowCredsStr := getEnv("GATEWAY_CORS_ALLOW_CREDENTIALS", "true")
	allowCreds := allowCredsStr != "false" && allowCredsStr != "0"

	var corsHandler *cors.Cors
	if originsCsv != "" {
		var origins []string
		for _, o := range strings.Split(originsCsv, ",") {
			if s := strings.TrimSpace(o); s != "" {
				origins = append(origins, s)
			}
		}
		corsHandler = cors.New(cors.Options{
			AllowedOrigins:   origins,
			AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
			AllowedHeaders:   []string{"Authorization", "Content-Type", "Accept"},
			ExposedHeaders:   []string{"Content-Type", "Content-Length", "Set-Cookie"},
			AllowCredentials: allowCreds,
		})
	} else {
		corsHandler = cors.AllowAll()
	}

	return corsHandler.Handler(handler)
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
