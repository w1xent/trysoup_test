package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"soupqueue/internal/queue"
)

func Handler(s *queue.Store, tokens map[string]string) (http.Handler, error) {
	m := http.NewServeMux()
	m.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		var ready bool
		err := s.DB.QueryRowContext(r.Context(), "SELECT to_regclass('jobs') IS NOT NULL AND to_regclass('effects') IS NOT NULL").Scan(&ready)
		if err != nil || !ready {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		reply(w, http.StatusOK, map[string]bool{"ready": true})
	})
	m.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if !validText(key) {
			problem(w, http.StatusBadRequest, "Idempotency-Key must contain 1 to 200 bytes")
			return
		}
		var spec queue.Spec
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&spec); err != nil || dec.Decode(&struct{}{}) != io.EOF ||
			!validText(spec.Name) || spec.DurationMS < 1 || spec.DurationMS > 60000 {
			problem(w, http.StatusBadRequest, "expected name (1 to 200 bytes), duration_ms (1 to 60000) and optional should_fail")
			return
		}
		j, created, err := authenticatedStore(s, r).Create(r.Context(), key, spec)
		if err != nil {
			dbError(w, err)
			return
		}
		code := http.StatusOK
		if created {
			code = http.StatusCreated
		}
		w.Header().Set("Location", "/jobs/"+strconv.FormatInt(j.ID, 10))
		reply(w, code, j)
	})
	m.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		after, limit := int64(0), int64(50)
		var err error
		if q.Has("after") {
			after, err = strconv.ParseInt(q.Get("after"), 10, 64)
		}
		if err == nil && q.Has("limit") {
			limit, err = strconv.ParseInt(q.Get("limit"), 10, 32)
		}
		if err != nil || after < 0 || limit < 1 || limit > 100 {
			problem(w, http.StatusBadRequest, "after must be nonnegative and limit between 1 and 100")
			return
		}
		items, err := authenticatedStore(s, r).List(r.Context(), after, int(limit)+1)
		if err != nil {
			dbError(w, err)
			return
		}
		var next *int64
		if len(items) > int(limit) {
			items = items[:limit]
			next = &items[len(items)-1].ID
		}
		reply(w, http.StatusOK, struct {
			Items []queue.Job `json:"items"`
			Next  *int64      `json:"next_after"`
		}{items, next})
	})
	for pattern, lookup := range map[string]funcID{
		"GET /jobs/{id}": (*queue.Store).Get, "POST /jobs/{id}/cancel": (*queue.Store).Cancel,
	} {
		m.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
			if err != nil || id < 1 {
				problem(w, http.StatusBadRequest, "invalid job id")
				return
			}
			j, err := lookup(authenticatedStore(s, r), r.Context(), id)
			if err != nil {
				dbError(w, err)
				return
			}
			reply(w, http.StatusOK, j)
		})
	}
	return protect(m, tokens)
}

type funcID func(*queue.Store, context.Context, int64) (queue.Job, error)

func validText(s string) bool {
	return strings.TrimSpace(s) != "" && len(s) <= 200 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

func reply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write response: %v", err)
	}
}

func problem(w http.ResponseWriter, status int, message string) {
	reply(w, status, map[string]string{"error": message})
}

func dbError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrQueueFull):
		w.Header().Set("Retry-After", "1")
		problem(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, queue.ErrConflict):
		problem(w, http.StatusConflict, err.Error())
	case errors.Is(err, sql.ErrNoRows):
		problem(w, http.StatusNotFound, "job not found")
	default:
		log.Printf("database request: %v", err)
		problem(w, http.StatusServiceUnavailable, "database request failed")
	}
}
