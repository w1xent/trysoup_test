package queue

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"
)

const Lease = 1500 * time.Millisecond

func Work(ctx context.Context, s *Store) {
	for ctx.Err() == nil {
		job, err := s.Claim(ctx, Lease)
		if err == nil {
			err = execute(ctx, s, job)
			if err == nil || errors.Is(err, ErrLostLease) {
				continue
			}
		}
		delay := 25 * time.Millisecond
		if !errors.Is(err, sql.ErrNoRows) && ctx.Err() == nil {
			log.Printf("worker job=%d attempt=%d: %v", job.ID, job.Attempt, err)
			delay = 250 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func execute(ctx context.Context, s *Store, job Job) error {
	done := time.NewTimer(time.Duration(job.DurationMS) * time.Millisecond)
	defer done.Stop()
	heartbeat := time.NewTicker(Lease / 3)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeat.C:
			if err := s.Renew(ctx, job, Lease); err != nil {
				return err
			}
		case <-done.C:
			return s.Finish(ctx, job)
		}
	}
}
