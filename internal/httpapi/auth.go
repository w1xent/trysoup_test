package httpapi

import (
	"context"
	"crypto/sha256"
	"errors"
	"mime"
	"net/http"
	"strings"

	"soupqueue/internal/queue"
)

type principalKey struct{}

// authenticatedStore only receives contexts populated by the authentication wrapper.
func authenticatedStore(s *queue.Store, r *http.Request) *queue.Store {
	return s.ForOwner(r.Context().Value(principalKey{}).(string))
}

func protect(next http.Handler, tokens map[string]string) (http.Handler, error) {
	if len(tokens) == 0 {
		return nil, errors.New("at least one API token is required")
	}
	owners := make(map[[32]byte]string, len(tokens))
	for owner, token := range tokens {
		if !validText(owner) || owner == "legacy" || len(token) < 32 {
			return nil, errors.New("API owners must be nonempty, distinct from legacy; tokens need at least 32 bytes")
		}
		digest := sha256.Sum256([]byte(token))
		if _, exists := owners[digest]; exists {
			return nil, errors.New("API tokens must be unique")
		}
		owners[digest] = owner
	}
	auth := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		owner, found := owners[sha256.Sum256([]byte(token))]
		if !ok || !found {
			w.Header().Set("WWW-Authenticate", "Bearer")
			problem(w, http.StatusUnauthorized, "valid Bearer token required")
			return
		}
		if r.Method == "POST" {
			kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || kind != "application/json" {
				problem(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
				return
			}
		}
		ctx := context.WithValue(r.Context(), principalKey{}, owner)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
	return http.NewCrossOriginProtection().Handler(auth), nil
}
