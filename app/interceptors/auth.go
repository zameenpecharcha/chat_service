package interceptors

import (
	"context"
	"crypto/rsa"
	"encoding/pem"
	"os"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// contextKey is an unexported type for context keys set by this package.
type contextKey int

const (
	// ContextKeyUserID is the context key for the authenticated user ID string.
	ContextKeyUserID contextKey = iota
)

// AuthInterceptor verifies RS256 JWT tokens on every incoming RPC.
// It mirrors the behaviour of user_service/app/interceptors/auth_interceptor.py:
//   - Token must be in the "authorization" metadata header as "Bearer <token>"
//   - Algorithm: RS256
//   - Audience:  configured (default "graphql-api")
//   - Issuer:    configured (default "ZPC")
//
// Health-check and gRPC reflection methods are exempted from auth.
type AuthInterceptor struct {
	publicKey *rsa.PublicKey
	audience  string
	issuer    string
}

// NewAuthInterceptor reads an RSA public key PEM file and constructs an interceptor.
// keyPath is typically "config/public.pem" — the same file used by user_service.
func NewAuthInterceptor(keyPath, audience, issuer string) (*AuthInterceptor, error) {
	raw, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, status.Error(codes.Internal, "failed to decode PEM block from public key file")
	}
	pub, err := jwt.ParseRSAPublicKeyFromPEM(raw)
	if err != nil {
		return nil, err
	}
	return &AuthInterceptor{publicKey: pub, audience: audience, issuer: issuer}, nil
}

// exempt returns true for methods that do not require authentication.
func exempt(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health") ||
		strings.HasPrefix(fullMethod, "/grpc.reflection")
}

// verify parses and validates a JWT token string.
// Returns the subject claim (user ID) on success.
func (a *AuthInterceptor) verify(tokenStr string) (string, error) {
	token, err := jwt.Parse(tokenStr,
		func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, status.Errorf(codes.Unauthenticated,
					"unexpected signing method: %v", t.Header["alg"])
			}
			return a.publicKey, nil
		},
		jwt.WithAudience(a.audience),
		jwt.WithIssuer(a.issuer),
		jwt.WithValidMethods([]string{"RS256"}),
	)
	if err != nil {
		return "", status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return "", status.Error(codes.Unauthenticated, "invalid token claims")
	}

	// user_service stores the user ID as the "sub" claim (standard JWT subject)
	sub, _ := claims["sub"].(string)
	return sub, nil
}

// extractToken returns the raw token string from the gRPC metadata.
func extractToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}
	raw := strings.TrimPrefix(vals[0], "Bearer ")
	raw = strings.TrimPrefix(raw, "bearer ")
	if raw == "" {
		return "", status.Error(codes.Unauthenticated, "empty token")
	}
	return raw, nil
}

// UnaryServerInterceptor returns a gRPC unary interceptor that enforces JWT auth.
// Used for CreateRoom, RequestUpload, GetMessages, GetPresence, GetDownloadUrl.
func (a *AuthInterceptor) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if exempt(info.FullMethod) {
			return handler(ctx, req)
		}
		tokenStr, err := extractToken(ctx)
		if err != nil {
			return nil, err
		}
		userID, err := a.verify(tokenStr)
		if err != nil {
			return nil, err
		}
		return handler(context.WithValue(ctx, ContextKeyUserID, userID), req)
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor that enforces JWT auth.
// Used for the Chat bidirectional-streaming RPC.
func (a *AuthInterceptor) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if exempt(info.FullMethod) {
			return handler(srv, ss)
		}
		tokenStr, err := extractToken(ss.Context())
		if err != nil {
			return err
		}
		userID, err := a.verify(tokenStr)
		if err != nil {
			return err
		}
		// Wrap the stream so its Context() returns the enriched context.
		return handler(srv, &wrappedStream{ServerStream: ss,
			ctx: context.WithValue(ss.Context(), ContextKeyUserID, userID)})
	}
}

// wrappedStream overrides Context() to carry extra values.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// UserIDFromContext extracts the authenticated user ID from a request context.
// Returns "" when auth is disabled or the value was not set.
func UserIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ContextKeyUserID).(string)
	return v
}
