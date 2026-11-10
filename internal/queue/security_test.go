package queue_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"soupqueue/internal/config"
	"soupqueue/internal/httpapi"
	"soupqueue/internal/queue"
)

func TestAuthenticationAndOwnership(t *testing.T) {
	s, _ := database(t)
	tokens := map[string]string{"alice": testToken, "bob": "test-token-bob-012345678901234567890123456789"}
	handler, err := httpapi.Handler(s, tokens)
	must(t, err)
	server := httptest.NewServer(handler)
	defer server.Close()
	call := func(method, path, owner, origin, content string, want int) []byte {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, strings.NewReader(content))
		must(t, err)
		req.Header.Set("Idempotency-Key", "same-key")
		if owner != "" {
			req.Header.Set("Authorization", "Bearer "+tokens[owner])
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if method == "POST" {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := server.Client().Do(req)
		must(t, err)
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		must(t, err)
		if res.StatusCode != want {
			t.Fatalf("%s %s got %d %s want %d", method, path, res.StatusCode, body, want)
		}
		return body
	}
	if _, err := httpapi.Handler(s, nil); err == nil {
		t.Fatal("empty authentication configuration accepted")
	}
	const body = `{"name":"private","duration_ms":60000}`
	call("GET", "/jobs", "", "", "", 401)
	call("POST", "/jobs", "", "", body, 401)
	var alice, bob queue.Job
	must(t, json.Unmarshal(call("POST", "/jobs", "alice", "", body, 201), &alice))
	must(t, json.Unmarshal(call("POST", "/jobs", "bob", "", body, 201), &bob))
	if alice.ID == bob.ID {
		t.Fatal("idempotency leaked across owners")
	}
	path := fmt.Sprintf("/jobs/%d", alice.ID)
	call("GET", path, "bob", "", "", 404)
	call("POST", path+"/cancel", "bob", "", "", 404)
	call("POST", path+"/cancel", "", "", "", 401)
	// A bearer-authenticated browser request is still subject to origin checks.
	call("POST", path+"/cancel", "alice", "http://attacker.local", "", 403)
	current, err := s.ForOwner("alice").Get(t.Context(), alice.ID)
	must(t, err)
	if current.State != "queued" {
		t.Fatal("cross-origin cancellation changed job")
	}
	var page struct{ Items []queue.Job }
	must(t, json.Unmarshal(call("GET", "/jobs", "bob", "", "", 200), &page))
	if len(page.Items) != 1 || page.Items[0].ID != bob.ID {
		t.Fatal("another owner's jobs exposed", page)
	}
	req, err := http.NewRequest("POST", server.URL+path+"/cancel", strings.NewReader("x=1"))
	must(t, err)
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := server.Client().Do(req)
	must(t, err)
	res.Body.Close()
	if res.StatusCode != 415 {
		t.Fatal("form submission accepted", res.StatusCode)
	}
	call("POST", path+"/cancel", "alice", "", "", 200)
}

func TestConcurrentAdmissionAndReplayAtCapacity(t *testing.T) {
	s, _ := database(t)
	alice := s.ForOwner("alice")
	var wg sync.WaitGroup
	start := make(chan struct{})
	var mu sync.Mutex
	accepted, rejected := 0, 0
	for i := range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := alice.Create(t.Context(), fmt.Sprint(i), queue.Spec{Name: "long", DurationMS: 60000})
			mu.Lock()
			defer mu.Unlock()
			switch err {
			case nil:
				accepted++
			case queue.ErrQueueFull:
				rejected++
			default:
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if accepted != 2 || rejected != 10 {
		t.Fatalf("accepted=%d rejected=%d", accepted, rejected)
	}
	jobs, err := alice.List(t.Context(), 0, 10)
	must(t, err)
	j, created, err := alice.Create(t.Context(), jobs[0].Key, jobs[0].Spec)
	must(t, err)
	if created || j.ID != jobs[0].ID {
		t.Fatal("replay unavailable at capacity")
	}
	_, _, err = s.ForOwner("bob").Create(t.Context(), "other", queue.Spec{Name: "other", DurationMS: 60000})
	must(t, err)
	_, err = alice.Cancel(t.Context(), jobs[0].ID)
	must(t, err)
	_, _, err = alice.Create(t.Context(), "replacement", queue.Spec{Name: "long", DurationMS: 60000})
	must(t, err)
}

func TestQueueCountLimit(t *testing.T) {
	s, _ := database(t)
	s = s.ForOwner("alice")
	for i := range queue.MaxOwnerPending {
		_, _, err := s.Create(t.Context(), fmt.Sprint(i), spec)
		must(t, err)
	}
	_, _, err := s.Create(t.Context(), "overflow", spec)
	if err != queue.ErrQueueFull {
		t.Fatal("count limit failed", err)
	}
}

func TestApplicationDatabaseRoles(t *testing.T) {
	raw, err := config.Value("TEST_DATABASE_URL")
	if err != nil {
		t.Skip(err)
	}
	s, err := queue.Open(raw)
	must(t, err)
	defer s.DB.Close()
	for _, role := range []string{"queue_api", "queue_worker"} {
		var elevated bool
		must(t, s.DB.QueryRow(`SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls
			FROM pg_roles WHERE rolname=$1`, role).Scan(&elevated))
		if elevated {
			t.Fatal(role, "has administrative privileges")
		}
		var canCreate bool
		must(t, s.DB.QueryRow("SELECT has_schema_privilege($1,'public','CREATE')", role).Scan(&canCreate))
		if canCreate {
			t.Fatal(role, "can create schema objects")
		}
	}
	var apiWritesEffects, workerCreatesJobs, encrypted bool
	must(t, s.DB.QueryRow("SELECT has_table_privilege('queue_api','public.effects','INSERT')").Scan(&apiWritesEffects))
	must(t, s.DB.QueryRow("SELECT has_table_privilege('queue_worker','public.jobs','INSERT')").Scan(&workerCreatesJobs))
	must(t, s.DB.QueryRow("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()").Scan(&encrypted))
	if apiWritesEffects || workerCreatesJobs || !encrypted {
		t.Fatalf("apiEffects=%v workerJobs=%v tls=%v", apiWritesEffects, workerCreatesJobs, encrypted)
	}
}

func TestGlobalQueueLimits(t *testing.T) {
	for _, kind := range []string{"count", "duration"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := database(t)
			n, duration := queue.MaxTotalPending, 1
			if kind == "duration" {
				n, duration = 10, 60000
			}
			_, err := s.DB.Exec(`INSERT INTO jobs(owner_id,request_key,name,duration_ms,should_fail)
				SELECT 'owner-'||i,i::text,'busy',$2,false FROM generate_series(1,$1) i`, n, duration)
			must(t, err)
			_, _, err = s.ForOwner("new-owner").Create(t.Context(), "overflow", spec)
			if err != queue.ErrQueueFull {
				t.Fatal("global limit not enforced", err)
			}
		})
	}
}

func TestLegacySchemaMigration(t *testing.T) {
	s, _ := database(t)
	j := create(t, s, "preserved")
	_, err := s.DB.Exec("ALTER TABLE jobs DROP COLUMN owner_id; ALTER TABLE jobs ADD CONSTRAINT jobs_request_key_key UNIQUE(request_key)")
	must(t, err)
	for range 2 {
		_, err = s.DB.Exec(schema)
		must(t, err)
	}
	current, err := s.Get(t.Context(), j.ID)
	must(t, err)
	if current.ID != j.ID || current.Key != j.Key || current.Spec != j.Spec {
		t.Fatal("legacy data changed", current)
	}
	_, err = s.ForOwner("alice").Get(t.Context(), j.ID)
	if err != sql.ErrNoRows {
		t.Fatal("legacy job exposed to a new owner", err)
	}
}

func TestHTTPReturns429AtCapacity(t *testing.T) {
	s, _ := database(t)
	owner := s.ForOwner("alice")
	long := queue.Spec{Name: "long", DurationMS: 60000}
	for _, key := range []string{"first", "second"} {
		_, _, err := owner.Create(t.Context(), key, long)
		must(t, err)
	}
	handler, err := httpapi.Handler(s, map[string]string{"alice": testToken})
	must(t, err)
	for _, sample := range []struct {
		key  string
		code int
	}{{"third", 429}, {"first", 200}} {
		req := httptest.NewRequest("POST", "/jobs", strings.NewReader(`{"name":"long","duration_ms":60000}`))
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", sample.key)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != sample.code {
			t.Fatalf("%s got %d want %d", sample.key, w.Code, sample.code)
		}
		if w.Code == 429 && w.Header().Get("Retry-After") != "1" {
			t.Fatal("missing retry guidance")
		}
	}
}
