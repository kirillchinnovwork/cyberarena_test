package main

import (
	"context"
	"encoding/json"
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

	gatewayfile "github.com/black-06/grpc-gateway-file"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/cors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type ctxKey string

const (
	userIDKey ctxKey = "userID"
	teamIDKey ctxKey = "teamID"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	usersAddr := getEnv("USERS_GRPC_ADDR", "users:50051")
	newsAddr := getEnv("NEWS_GRPC_ADDR", "news:50052")
	polygonAddr := getEnv("POLYGON_GRPC_ADDR", "polygon:50054")
	attachmentsAddr := getEnv("ATTACHMENTS_GRPC_ADDR", "attachments:50055")
	authAddr := getEnv("AUTH_GRPC_ADDR", "auth:50053")

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	authConn, err := grpc.DialContext(ctx, authAddr, dialOpts...)
	if err != nil {
		log.Fatalf("dial auth: %v", err)
	}
	defer authConn.Close()
	authClient := authv1.NewAuthClientServiceClient(authConn)

	// Имя refresh-cookie можно настроить через env (должно совпадать с сервисом auth)
	refreshCookieName := getEnv("AUTH_REFRESH_COOKIE_NAME", "refresh_token")

	mux := runtime.NewServeMux(
		gatewayfile.WithFileIncomingHeaderMatcher(),
		gatewayfile.WithFileForwardResponseOption(),
		gatewayfile.WithHTTPBodyMarshaler(),

		runtime.WithMetadata(func(c context.Context, r *http.Request) metadata.MD {
			md := metadata.MD{}
			if uid, ok := r.Context().Value(userIDKey).(string); ok && uid != "" {
				md.Append("x-user-id", uid)
			}
			if tid, ok := r.Context().Value(teamIDKey).(string); ok && tid != "" {
				md.Append("x-team-id", tid)
			}

			if authz := r.Header.Get("Authorization"); authz != "" {
				md.Append("authorization", authz)
			}
			// Прокидываем refresh-токен из httpOnly cookie в metadata для gRPC
			if c, err := r.Cookie(refreshCookieName); err == nil && c != nil && c.Value != "" {
				md.Append("x-refresh-token", c.Value)
			}
			if len(md) == 0 {
				return nil
			}
			return md
		}),

		// Форвардим Set-Cookie из gRPC в HTTP-ответ
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
		log.Fatalf("register users handler: %v", err)
	}
	if err := newsv1.RegisterNewsClientServiceHandlerFromEndpoint(ctx, mux, newsAddr, dialOpts); err != nil {
		log.Fatalf("register news handler: %v", err)
	}
	if err := polygonv1.RegisterPolygonClientServiceHandlerFromEndpoint(ctx, mux, polygonAddr, dialOpts); err != nil {
		log.Fatalf("register polygon handler: %v", err)
	}
	if err := authv1.RegisterAuthClientServiceHandlerFromEndpoint(ctx, mux, authAddr, dialOpts); err != nil {
		log.Fatalf("register auth handler: %v", err)
	}
	if err := attv1.RegisterAttachmentsClientServiceHandlerFromEndpoint(ctx, mux, attachmentsAddr, dialOpts); err != nil {
		log.Fatalf("register attachments handler: %v", err)
	}

	root := authMiddleware(authClient, mux)

	httpAddr := getEnv("GATEWAY_HTTP_ADDR", ":8080")

	// CORS с поддержкой cookie
	originsCsv := getEnv("GATEWAY_CORS_ORIGINS", "") // пример: https://app.example.com,https://admin.example.com
	allowCredsStr := getEnv("GATEWAY_CORS_ALLOW_CREDENTIALS", "true")
	allowCreds := true
	if allowCredsStr == "false" || allowCredsStr == "0" {
		allowCreds = false
	}
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

	srv := &http.Server{Addr: httpAddr, Handler: corsHandler.Handler(root), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("gateway HTTP listening on %s", httpAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gateway failed: %v", err)
	}
}

func authMiddleware(authClient authv1.AuthClientServiceClient, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		if authz == "" {
			// TODO: Закрыть эту хрень
			next.ServeHTTP(w, r)
			return
		}
		parts := strings.SplitN(authz, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
			writeAuthError(w, http.StatusUnauthorized, "invalid_authorization_header")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		resp, err := authClient.ValidateToken(ctx, &authv1.ValidateTokenRequest{AccessToken: parts[1]})
		if err != nil {
			writeAuthError(w, http.StatusUnauthorized, "invalid_token")
			return
		}
		ctx = context.WithValue(r.Context(), userIDKey, resp.GetUserId())
		if resp.GetTeamId() != "" {
			ctx = context.WithValue(ctx, teamIDKey, resp.GetTeamId())
		}
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	})
}

func writeAuthError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
