package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

type ctxKey string

const (
	UserIDKey ctxKey = "userID"
	TeamIDKey ctxKey = "teamID"
	RoleKey   ctxKey = "role"
)

type Role string

const (
	RoleUser  Role = "user"
	RoleAdmin Role = "admin"
)

type Claims struct {
	TeamID string `json:"team_id,omitempty"`
	Role   Role   `json:"role,omitempty"`
	jwt.RegisteredClaims
}

type AuthMiddleware struct {
	jwtSecret   []byte
	publicPaths []string
}

func NewAuthMiddleware(jwtSecret []byte) *AuthMiddleware {
	return &AuthMiddleware{
		jwtSecret: jwtSecret,
		publicPaths: []string{
			"/v1/auth/login",
			"/v1/auth/register",
			"/v1/auth/refresh",
		},
	}
}

func (m *AuthMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, p := range m.publicPaths {
			if strings.HasPrefix(r.URL.Path, p) {
				next.ServeHTTP(w, r)
				return
			}
		}

		authz := r.Header.Get("Authorization")
		if authz == "" {
			next.ServeHTTP(w, r)
			return
		}

		parts := strings.SplitN(authz, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
			writeAuthError(w, http.StatusUnauthorized, "invalid_authorization_header")
			return
		}

		claims, err := m.validateToken(parts[1])
		if err != nil {
			writeAuthError(w, http.StatusUnauthorized, "invalid_token")
			return
		}

		if strings.HasPrefix(r.URL.Path, "/v1/admin/") {
			if claims.Role != RoleAdmin {
				writeAuthError(w, http.StatusForbidden, "admin_access_required")
				return
			}
		}

		ctx := context.WithValue(r.Context(), UserIDKey, claims.Subject)
		if claims.TeamID != "" {
			ctx = context.WithValue(ctx, TeamIDKey, claims.TeamID)
		}
		ctx = context.WithValue(ctx, RoleKey, claims.Role)

		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	})
}

func (m *AuthMiddleware) validateToken(tokenString string) (*Claims, error) {
	parsed, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, errors.New("unexpected signing method")
		}
		return m.jwtSecret, nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := parsed.Claims.(*Claims)
	if !ok || !parsed.Valid || claims.Subject == "" {
		return nil, errors.New("invalid token claims")
	}

	if claims.Role == "" {
		claims.Role = RoleUser
	}

	return claims, nil
}

func writeAuthError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func GetUserID(ctx context.Context) string {
	if v, ok := ctx.Value(UserIDKey).(string); ok {
		return v
	}
	return ""
}

func GetTeamID(ctx context.Context) string {
	if v, ok := ctx.Value(TeamIDKey).(string); ok {
		return v
	}
	return ""
}

func GetRole(ctx context.Context) Role {
	if v, ok := ctx.Value(RoleKey).(Role); ok {
		return v
	}
	return RoleUser
}

func IsAdmin(ctx context.Context) bool {
	return GetRole(ctx) == RoleAdmin
}
