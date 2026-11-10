package queue_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"soupqueue/internal/config"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"soupqueue/internal/queue"
)

//go:embed schema.sql
var schema string

// Every test gets a private schema, even when a normal worker shares the database.
func database(t *testing.T) (*queue.Store, string) {
	t.Helper()
	raw, configErr := config.Value("TEST_DATABASE_URL")
	if configErr != nil {
		t.Skip("integration test requires TEST_DATABASE_URL")
	}
	admin, err := queue.Open(raw)
	must(t, err)
	t.Cleanup(func() { admin.DB.Close() })
	var entropy [8]byte
	_, err = rand.Read(entropy[:])
	must(t, err)
	ns := fmt.Sprintf("test_%x", entropy)
	_, err = admin.DB.Exec("CREATE SCHEMA " + ns)
	must(t, err)
	t.Cleanup(func() {
		_, err := admin.DB.Exec("DROP SCHEMA " + ns + " CASCADE")
		if err != nil {
			t.Error(err)
		}
	})
	u, err := url.Parse(raw)
	must(t, err)
	q := u.Query()
	q.Set("search_path", ns)
	u.RawQuery = q.Encode()
	s, err := queue.Open(u.String())
	must(t, err)
	t.Cleanup(func() { s.DB.Close() })
	_, err = s.DB.Exec(schema)
	must(t, err)
	return s, u.String()
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

var spec = queue.Spec{Name: "model", DurationMS: 10}

func create(t *testing.T, s *queue.Store, key string) queue.Job {
	t.Helper()
	j, ok, err := s.Create(t.Context(), key, spec)
	must(t, err)
	if !ok {
		t.Fatal("expected new job", key)
	}
	return j
}

func claim(t *testing.T, s *queue.Store, lease time.Duration) queue.Job {
	t.Helper()
	j, err := s.Claim(t.Context(), lease)
	must(t, err)
	return j
}

func assertState(t *testing.T, s *queue.Store, id int64, want string, effects int) {
	t.Helper()
	var state string
	var count int
	var cleared bool
	must(t, s.DB.QueryRow(`SELECT state, lease_until IS NULL, (SELECT count(*) FROM effects WHERE job_id=j.id)
		FROM jobs j WHERE id=$1`, id).Scan(&state, &cleared, &count))
	if state != want || count != effects || cleared != (state != "running") {
		t.Fatalf("state=%s effects=%d cleared=%v", state, count, cleared)
	}
}

func expire(t *testing.T, s *queue.Store, id int64) {
	t.Helper()
	_, err := s.DB.Exec("UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", id)
	must(t, err)
}

func TestConcurrentClaimAndSkipLocked(t *testing.T) {
	s, _ := database(t)
	first := create(t, s, "locked")
	second := create(t, s, "available")
	tx, err := s.DB.Begin()
	must(t, err)
	defer tx.Rollback()
	_, err = tx.Exec("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", first.ID)
	must(t, err)
	start := make(chan struct{})
	type result struct {
		job queue.Job
		err error
	}
	results := make(chan result, 8)
	for range 8 {
		go func() { <-start; j, err := s.Claim(t.Context(), time.Minute); results <- result{j, err} }()
	}
	close(start)
	winners := 0
	for range 8 {
		r := <-results
		if errors.Is(r.err, sql.ErrNoRows) {
			continue
		}
		must(t, r.err)
		if r.job.ID != second.ID || r.job.Attempt != 1 {
			t.Fatal(r.job)
		}
		winners++
	}
	if winners != 1 {
		t.Fatalf("winners=%d", winners)
	}
	must(t, tx.Rollback())
	if claim(t, s, time.Minute).ID != first.ID {
		t.Fatal("skipped row lost")
	}
}

func TestExpiredZombie(t *testing.T) {
	s, _ := database(t)
	create(t, s, "zombie")
	old := claim(t, s, time.Minute)
	expire(t, s, old.ID)
	rejectOld := func(phase string) {
		t.Helper()
		if err := s.Finish(t.Context(), old); !errors.Is(err, queue.ErrLostLease) {
			t.Fatalf("%s finish: %v", phase, err)
		}
		if err := s.Renew(t.Context(), old, time.Minute); !errors.Is(err, queue.ErrLostLease) {
			t.Fatalf("%s renew: %v", phase, err)
		}
		assertState(t, s, old.ID, "running", 0)
	}
	rejectOld("expired")
	fresh := claim(t, s, time.Minute)
	if fresh.ID != old.ID || fresh.Attempt != old.Attempt+1 {
		t.Fatal(fresh)
	}
	rejectOld("reclaimed")
	must(t, s.Finish(t.Context(), fresh))
	assertState(t, s, old.ID, "succeeded", 1)
	if err := s.Finish(t.Context(), fresh); !errors.Is(err, queue.ErrLostLease) {
		t.Fatal(err)
	}
}

// Observe the actual lock wait before releasing the competing transaction.
func waitForBlocked(t *testing.T, s *queue.Store, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		must(t, s.DB.QueryRow("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))", pid).Scan(&blocked))
		if blocked {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("did not observe a PostgreSQL lock wait")
}

func beginBlocker(t *testing.T, s *queue.Store) (*sql.Tx, int) {
	t.Helper()
	tx, err := s.DB.Begin()
	must(t, err)
	t.Cleanup(func() { tx.Rollback() })
	var pid int
	must(t, tx.QueryRow("SELECT pg_backend_pid()").Scan(&pid))
	return tx, pid
}

func TestLeaseExpiresDuringRowLockWait(t *testing.T) {
	s, _ := database(t)
	create(t, s, "wait")
	j := claim(t, s, queue.Lease)
	tx, pid := beginBlocker(t, s)
	_, err := tx.Exec("SELECT FROM jobs WHERE id=$1 FOR UPDATE", j.ID)
	must(t, err)
	// No update by the blocker: a pre-lock predicate would stay stale.
	done := make(chan error, 1)
	go func() { done <- s.Finish(t.Context(), j) }()
	waitForBlocked(t, s, pid)
	var remaining float64
	must(t, tx.QueryRow("SELECT extract(epoch FROM lease_until-clock_timestamp()) FROM jobs WHERE id=$1", j.ID).Scan(&remaining))
	if remaining <= 0 {
		t.Fatal("fixture did not enter lock wait before expiry")
	}
	time.Sleep(time.Duration(remaining*float64(time.Second)) + 20*time.Millisecond)
	must(t, tx.Rollback())
	if err := <-done; !errors.Is(err, queue.ErrLostLease) {
		t.Fatal("stale result accepted", err)
	}
	assertState(t, s, j.ID, "running", 0)
	fresh := claim(t, s, time.Minute)
	must(t, s.Finish(t.Context(), fresh))
}

func TestIdempotencyAcrossUncommittedInsert(t *testing.T) {
	s, _ := database(t)
	var isolation string
	must(t, s.DB.QueryRow("SHOW transaction_isolation").Scan(&isolation))
	if isolation != "read committed" {
		t.Fatal(isolation)
	}
	tx, pid := beginBlocker(t, s)
	var id int64
	must(t, tx.QueryRow("INSERT INTO jobs(request_key,name,duration_ms,should_fail) VALUES('retry','model',10,false) RETURNING id").Scan(&id))
	type result struct {
		job     queue.Job
		created bool
		err     error
	}
	done := make(chan result, 1)
	go func() { j, created, err := s.Create(t.Context(), "retry", spec); done <- result{j, created, err} }()
	waitForBlocked(t, s, pid)
	must(t, tx.Commit())
	r := <-done
	must(t, r.err)
	if r.created || r.job.ID != id {
		t.Fatal(r)
	}
	_, _, err := s.Create(t.Context(), "retry", queue.Spec{Name: "other", DurationMS: 10})
	if !errors.Is(err, queue.ErrConflict) {
		t.Fatal(err)
	}
}

func TestCancelCompleteRace(t *testing.T) {
	s, _ := database(t)
	wins := map[string]int{}
	for i := range 48 {
		j := create(t, s, fmt.Sprint(i))
		j = claim(t, s, time.Minute)
		done := make(chan error, 1)
		start := make(chan struct{})
		go func() { <-start; done <- s.Finish(t.Context(), j) }()
		// Include both serial orders plus 46 concurrent starts.
		if i == 0 {
			_, err := s.Cancel(t.Context(), j.ID)
			must(t, err)
		}
		close(start)
		var finishErr error
		if i == 1 {
			finishErr = <-done
		}
		cancelled, err := s.Cancel(t.Context(), j.ID)
		must(t, err)
		if i != 1 {
			finishErr = <-done
		}
		switch cancelled.State {
		case "cancelled":
			if !errors.Is(finishErr, queue.ErrLostLease) {
				t.Fatal(finishErr)
			}
			assertState(t, s, j.ID, "cancelled", 0)
		case "succeeded":
			must(t, finishErr)
			assertState(t, s, j.ID, "succeeded", 1)
		default:
			t.Fatal(cancelled)
		}
		wins[cancelled.State]++
	}
	t.Logf("cancel replies by winning state: %v", wins)
}

func TestEffectErrorRollsBackState(t *testing.T) {
	s, _ := database(t)
	create(t, s, "atomic")
	j := claim(t, s, time.Minute)
	_, err := s.DB.Exec("ALTER TABLE effects ADD CONSTRAINT injected_failure CHECK(false)")
	must(t, err)
	if err := s.Finish(t.Context(), j); err == nil {
		t.Fatal("effect write should have failed")
	}
	assertState(t, s, j.ID, "running", 0)
	_, err = s.DB.Exec("ALTER TABLE effects DROP CONSTRAINT injected_failure")
	must(t, err)
	must(t, s.Finish(t.Context(), j))
	assertState(t, s, j.ID, "succeeded", 1)
}

func TestWorkerClockSkew(t *testing.T) {
	_, dsn := database(t)
	for _, offset := range []time.Duration{0, 60 * 365 * 24 * time.Hour} {
		// Establish verified TLS with real wall time before changing the test clock.
		s, err := queue.Open(dsn)
		must(t, err)
		s.DB.SetMaxOpenConns(1)
		must(t, s.DB.PingContext(t.Context()))
		t.Cleanup(func() { s.DB.Close() })
		synctest.Test(t, func(t *testing.T) {
			time.Sleep(offset)
			var dbNow time.Time
			must(t, s.DB.QueryRow("SELECT clock_timestamp()").Scan(&dbNow))
			if time.Since(dbNow).Abs() < 365*24*time.Hour {
				t.Fatal("clock skew was not injected")
			}
			t.Logf("worker clock %s, PostgreSQL clock %s", time.Now().UTC(), dbNow.UTC())
			create(t, s, time.Now().String())
			j := claim(t, s, time.Minute)
			must(t, s.Renew(t.Context(), j, time.Minute))
			var remaining float64
			must(t, s.DB.QueryRow("SELECT extract(epoch FROM lease_until-clock_timestamp()) FROM jobs WHERE id=$1", j.ID).Scan(&remaining))
			if remaining < 55 || remaining > 60 {
				t.Fatal("lease derived from wrong clock", remaining)
			}
			must(t, s.Finish(t.Context(), j))
		})
	}
}

func TestHeartbeatDuringLongJob(t *testing.T) {
	s, _ := database(t)
	j, _, err := s.Create(t.Context(), "long", queue.Spec{Name: "long", DurationMS: 2600})
	must(t, err)
	ctx, stop := context.WithTimeout(t.Context(), 7*time.Second)
	var wg sync.WaitGroup
	defer func() { stop(); wg.Wait() }()
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); queue.Work(ctx, s) }()
	}
	for ctx.Err() == nil {
		current, err := s.Get(ctx, j.ID)
		must(t, err)
		if current.State == "succeeded" {
			if current.Attempt != 1 {
				t.Fatalf("healthy worker lost lease: %v", current)
			}
			assertState(t, s, j.ID, "succeeded", 1)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("worker did not complete")
}
