package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

func (e receipt) expectedState() string {
	if e.cancelState == "cancelled" {
		return "cancelled"
	}
	if e.spec.Fail {
		return "failed"
	}
	return "succeeded"
}

type stored struct {
	job
	NoLease       bool   `json:"no_lease"`
	Effects       int    `json:"effects"`
	EffectAttempt *int64 `json:"effect_attempt"`
}

func (r *run) verify(ctx context.Context, t *traffic, receipts []receipt, kills int) error {
	// One database snapshot. Count effect rows independently of terminal flags.
	raw, err := r.query(ctx, `SELECT row_to_json(v) FROM (
		SELECT j.request_key AS key,j.id,j.name,j.duration_ms,j.should_fail,j.state,j.attempt,
			j.lease_until IS NULL AS no_lease,count(e.job_id) AS effects,min(e.attempt) AS effect_attempt
		FROM jobs j LEFT JOIN effects e ON e.job_id=j.id GROUP BY j.id ORDER BY j.id
	) v`)
	if err != nil {
		return err
	}
	missing := make(map[string]receipt, len(receipts))
	for _, e := range receipts {
		missing[e.key] = e
	}
	states := map[string]int{}
	cancels := map[string]int{}
	var effects, attempts, retried int64
	for _, line := range strings.Split(raw, "\n") {
		var j stored
		if err := json.Unmarshal([]byte(line), &j); err != nil {
			return err
		}
		e, ok := missing[j.Key]
		if !ok || e.id != j.ID || e.spec != j.spec {
			return fmt.Errorf("unexpected, duplicate or changed job: %s", line)
		}
		delete(missing, j.Key)
		if !j.NoLease || j.State != e.expectedState() || (e.cancelState != "" && j.State != e.cancelState) {
			return fmt.Errorf("want state=%s cancel_ack=%s observed %s", e.expectedState(), e.cancelState, line)
		}
		wantEffects := 0
		if j.State == "succeeded" {
			wantEffects = 1
		}
		if j.Effects != wantEffects || (wantEffects == 1 && (j.EffectAttempt == nil || *j.EffectAttempt != j.Attempt)) ||
			(j.State != "cancelled" && j.Attempt < 1) {
			return fmt.Errorf("invalid effect or attempt: %s", line)
		}
		effects += int64(j.Effects)
		attempts += j.Attempt
		if j.Attempt > 1 {
			retried++
		}
		states[j.State]++
		if e.cancelState != "" {
			cancels[e.cancelState]++
		}
	}
	for key := range missing {
		return fmt.Errorf("missing %d keys, e.g. %s", len(missing), key)
	}
	if r.chaos && r.jobs >= 10000 && (kills == 0 || retried == 0 || len(states) != 3) {
		return fmt.Errorf("insufficient fault or outcome coverage kills=%d retried=%d states=%v", kills, retried, states)
	}
	if err := verifyAPI(ctx, t, receipts); err != nil {
		return err
	}
	r.log.Printf("verified keys=%d states=%v effects=%d attempts=%d retried_jobs=%d kills=%d cancels=%v paginated=%d missing=0 duplicate_effects=0 stuck=0 inconsistencies=0",
		len(receipts), states, effects, attempts, retried, kills, cancels, len(receipts))
	return nil
}

func verifyAPI(ctx context.Context, t *traffic, receipts []receipt) error {
	sorted := append([]receipt(nil), receipts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].id < sorted[j].id })
	var after int64
	count := 0
	for {
		var page struct {
			Items []job  `json:"items"`
			Next  *int64 `json:"next_after"`
		}
		_, err := t.request(ctx, "GET", fmt.Sprintf("/jobs?limit=100&after=%d", after), "", nil, time.Time{}, &page)
		if err != nil {
			return err
		}
		if len(page.Items) > 100 {
			return fmt.Errorf("page exceeded limit at %d", after)
		}
		for _, j := range page.Items {
			if count >= len(sorted) || sorted[count].id != j.ID || j.State != sorted[count].expectedState() || j.Key != sorted[count].key {
				return fmt.Errorf("pagination mismatch at %d: %+v", count, j)
			}
			count++
			after = j.ID
		}
		if page.Next == nil {
			break
		}
		if len(page.Items) == 0 || *page.Next != after || count >= len(sorted) {
			return fmt.Errorf("invalid cursor %d", *page.Next)
		}
	}
	if count != len(sorted) {
		return fmt.Errorf("pagination found %d of %d jobs", count, len(sorted))
	}
	for i := 0; i < len(receipts); i += 101 {
		e := receipts[i]
		var j job
		_, err := t.request(ctx, "GET", fmt.Sprintf("/jobs/%d", e.id), "", nil, time.Time{}, &j)
		if err != nil {
			return err
		}
		if j.ID != e.id || j.Key != e.key || j.State != e.expectedState() || j.spec != e.spec {
			return fmt.Errorf("status mismatch for %s: %+v", e.key, j)
		}
	}
	return nil
}
