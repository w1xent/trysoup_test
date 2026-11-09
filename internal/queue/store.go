package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	_ "github.com/lib/pq"
)

// Spec describes a job to be executed.
type Spec struct {
	Name       string `json:"name"`
	DurationMS int    `json:"duration_ms"`
	ShouldFail bool   `json:"should_fail"`
}

// Job represents a job in the queue.
type Job struct {
	ID  int64  `json:"id"`
	Key string `json:"key"`
	Spec
	State   string `json:"state"`
	Attempt int64  `json:"attempt"`
}

var ErrConflict = errors.New("key already used for a different job")
var ErrLostLease = errors.New("attempt no longer owns the lease")

type Store struct {
	DB *sql.DB
}

// Open opens a queue.Store using the given PostgreSQL DSN.
func Open(dsn string) (*Store, error) {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return nil, errors.New("DATABASE_URL must be a PostgreSQL URL")
	}
	params := u.Query()
	params.Set("default_transaction_isolation", "read committed")
	params.Set("statement_timeout", "5000")
	params.Set("idle_in_transaction_session_timeout", "5000")
	u.RawQuery = params.Encode()
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	return &Store{db}, nil
}

const columns = "id, request_key, name, duration_ms, should_fail, state, attempt"

// readJob reads a Job from the given row.
func readJob(row interface{ Scan(...any) error }) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Key, &j.Name, &j.DurationMS, &j.ShouldFail, &j.State, &j.Attempt)
	return j, err
}

// Create creates a new job with the given idempotency key and spec.
// If a job with the same key already exists, it returns that job instead.
// If the existing job has a different spec, it returns ErrConflict.
func (s *Store) Create(ctx context.Context, key string, spec Spec) (Job, bool, error) {
	j, err := readJob(s.DB.QueryRowContext(ctx, `
		INSERT INTO jobs(request_key, name, duration_ms, should_fail)
		VALUES ($1,$2,$3,$4) ON CONFLICT (request_key) DO NOTHING RETURNING `+columns,
		key, spec.Name, spec.DurationMS, spec.ShouldFail))
	if !errors.Is(err, sql.ErrNoRows) {
		return j, err == nil, err
	}
	// This statement takes a fresh snapshot after the losing insert has waited.
	j, err = readJob(s.DB.QueryRowContext(ctx, "SELECT "+columns+" FROM jobs WHERE request_key=$1", key))
	if err == nil && j.Spec != spec {
		return Job{}, false, ErrConflict
	}
	return j, false, err
}

// Get returns the job with the given ID.
func (s *Store) Get(ctx context.Context, id int64) (Job, error) {
	return readJob(s.DB.QueryRowContext(ctx, "SELECT "+columns+" FROM jobs WHERE id=$1", id))
}

// List returns up to limit jobs with IDs greater than after, ordered by ID.
func (s *Store) List(ctx context.Context, after int64, limit int) ([]Job, error) {
	rows, err := s.DB.QueryContext(ctx, "SELECT "+columns+" FROM jobs WHERE id>$1 ORDER BY id LIMIT $2", after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]Job, 0, limit)
	for rows.Next() {
		j, err := readJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Cancel cancels the job with the given ID if it is queued or running.
// If the job is already finished, it returns the job without changing its state.
func (s *Store) Cancel(ctx context.Context, id int64) (Job, error) {
	j, err := readJob(s.DB.QueryRowContext(ctx, `UPDATE jobs SET state='cancelled', lease_until=NULL
		WHERE id=$1 AND state IN ('queued','running') RETURNING `+columns, id))
	if errors.Is(err, sql.ErrNoRows) {
		return s.Get(ctx, id)
	}
	return j, err
}

// Claim returns a job that is queued or running and not currently leased,
// and marks it as running with a lease until the given duration from now. If no such job exists,
// it returns sql.ErrNoRows.
func (s *Store) Claim(ctx context.Context, lease time.Duration) (Job, error) {
	return readJob(s.DB.QueryRowContext(ctx, `
		WITH candidate AS (
			SELECT id FROM jobs WHERE state IN ('queued','running')
			AND (state='queued' OR lease_until <= clock_timestamp())
			ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs SET state='running', attempt=attempt+1,
			lease_until=clock_timestamp()+$1 * interval '1 millisecond'
		WHERE id=(SELECT id FROM candidate) RETURNING `+columns, lease.Milliseconds()))
}

const owned = " WHERE id=$1 AND attempt=$2 AND state='running' AND lease_until > clock_timestamp()"

// Acquire the row lock before reading the clock. A process blocked on the lock
// must not use a lease check it performed before waiting.
func (s *Store) withAttempt(ctx context.Context, job Job, action func(*sql.Tx) error) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id int64
	err = tx.QueryRowContext(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job.ID).Scan(&id)
	if err != nil {
		return err
	}
	if err := action(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// changed returns ErrLostLease if the result does not indicate that exactly one row was affected.
func changed(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrLostLease
	}
	return nil
}

// Renew extends the lease on the given job if it is still running and owned by the given attempt.
func (s *Store) Renew(ctx context.Context, job Job, lease time.Duration) error {
	return s.withAttempt(ctx, job, func(tx *sql.Tx) error {
		return changed(tx.ExecContext(ctx, `UPDATE jobs SET lease_until=clock_timestamp()+$3 * interval '1 millisecond'`+owned,
			job.ID, job.Attempt, lease.Milliseconds()))
	})
}

// Finish marks the job as succeeded or failed, depending on its
// ShouldFail field, and records an effect if it succeeded.
func (s *Store) Finish(ctx context.Context, job Job) error {
	return s.withAttempt(ctx, job, func(tx *sql.Tx) error {
		state := "succeeded"
		if job.ShouldFail {
			state = "failed"
		}
		err := changed(tx.ExecContext(ctx, "UPDATE jobs SET state=$3, lease_until=NULL"+owned, job.ID, job.Attempt, state))
		if err != nil || job.ShouldFail {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO effects(job_id, attempt) VALUES ($1,$2)", job.ID, job.Attempt)
		if err != nil {
			return fmt.Errorf("record effect: %w", err)
		}
		return nil
	})
}
