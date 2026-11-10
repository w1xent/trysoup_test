package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"
)

type spec struct {
	Name     string `json:"name"`
	Duration int    `json:"duration_ms"`
	Fail     bool   `json:"should_fail"`
}

type job struct {
	ID      int64  `json:"id"`
	Key     string `json:"key"`
	State   string `json:"state"`
	Attempt int64  `json:"attempt"`
	spec
}

type receipt struct {
	key         string
	spec        spec
	id          int64
	cancelState string
}

type traffic struct {
	base            string
	token           string
	client          *http.Client
	mu              sync.Mutex
	codes           map[int]int
	wire, scheduled []time.Duration
}

func newTraffic(base string) *traffic {
	return &traffic{base: base, client: &http.Client{Timeout: 10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: 64}}, codes: make(map[int]int)}
}

func (t *traffic) request(ctx context.Context, method, path, key string, body []byte, due time.Time, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, method, t.base+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+t.token)
	if method == "POST" {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
	}
	start := time.Now()
	res, err := t.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return res.StatusCode, err
	}
	if !due.IsZero() {
		end := time.Now()
		t.mu.Lock()
		t.wire = append(t.wire, end.Sub(start))
		t.scheduled = append(t.scheduled, end.Sub(due))
		t.codes[res.StatusCode]++
		t.mu.Unlock()
	}
	if res.StatusCode != 200 && res.StatusCode != 201 {
		return res.StatusCode, fmt.Errorf("%s %s key=%q status=%d body=%s", method, path, key, res.StatusCode, data)
	}
	return res.StatusCode, json.Unmarshal(data, out)
}

type response struct {
	job  job
	code int
	err  error
}

func (t *traffic) submit(ctx context.Context, want receipt, duplicate, cancel bool, due time.Time) (receipt, error) {
	body, err := json.Marshal(want.spec)
	if err != nil {
		return want, err
	}
	post := func() response {
		retryCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		defer cancel()
		for {
			var result response
			result.code, result.err = t.request(retryCtx, "POST", "/jobs", want.key, body, due, &result.job)
			if result.code != http.StatusTooManyRequests {
				if result.err == nil && (result.job.ID <= 0 || result.job.Key != want.key || result.job.spec != want.spec) {
					result.err = fmt.Errorf("unexpected create receipt: %+v", result.job)
				}
				return result
			}
			// Rejected submissions have not been accepted into the queue.
			select {
			case <-retryCtx.Done():
				return response{err: retryCtx.Err()}
			case <-time.After(time.Second):
			}
		}
	}
	// Simultaneous identical POSTs can arrive in either order.
	repeated := make(chan response, 1)
	if duplicate {
		go func() { repeated <- post() }()
	}
	first := post()
	want.id = first.job.ID
	if first.err == nil && cancel {
		var j job
		code, err := t.request(ctx, "POST", fmt.Sprintf("/jobs/%d/cancel", want.id), "", nil, due, &j)
		if err != nil {
			first.err = err
		} else if code != 200 || j.ID != want.id || (j.State != "cancelled" && j.State != "succeeded" && j.State != "failed") {
			first.err = fmt.Errorf("unexpected cancel receipt: %+v", j)
		} else {
			want.cancelState = j.State
		}
	}
	created := first.code == 201
	if duplicate {
		retry := <-repeated
		if retry.err != nil {
			return want, retry.err
		}
		if first.err == nil && (retry.job.ID != want.id || (retry.code == 201) == created) {
			return want, fmt.Errorf("retry mismatch: first=%+v retry=%+v", first, retry)
		}
	} else if first.err == nil && !created {
		return want, fmt.Errorf("no creation for %s", want.key)
	}
	return want, first.err
}

func percentiles(d []time.Duration) string {
	if len(d) == 0 {
		return "no samples"
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	q := func(p float64) float64 {
		return float64(d[int(math.Ceil(p*float64(len(d))))-1]) / float64(time.Millisecond)
	}
	return fmt.Sprintf("p50=%.2f p95=%.2f p99=%.2f max=%.2f", q(.50), q(.95), q(.99), q(1))
}

func (r *run) load(ctx context.Context, t *traffic) ([]receipt, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	manifest := make([]receipt, r.jobs)
	rng := rand.New(rand.NewSource(r.seed))
	slots := make(chan struct{}, 48)
	var wg sync.WaitGroup
	start := time.Now()
	offered := 0
dispatch:
	for i := range manifest {
		due := start.Add(time.Duration(float64(offered) / float64(r.rate) * float64(time.Second)))
		select {
		case <-ctx.Done():
			break dispatch
		case <-time.After(time.Until(due)):
		}
		select {
		case <-ctx.Done():
			break dispatch
		case slots <- struct{}{}:
		}
		duration := 650
		if i%40 != 0 {
			duration = 35 + rng.Intn(56)
		}
		want := receipt{key: fmt.Sprintf("training-%d", i), spec: spec{Name: fmt.Sprintf("model-%d", i), Duration: duration, Fail: i%23 == 0}}
		duplicate, cancelJob := r.chaos && i%3 == 0, r.chaos && i%5 == 0
		offered++
		if duplicate {
			offered++
		}
		if cancelJob {
			offered++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			result, err := t.submit(ctx, want, duplicate, cancelJob, due)
			if err != nil {
				cancel(err)
				return
			}
			manifest[i] = result
		}()
		if (i+1)%1000 == 0 {
			r.log.Printf("offered unique=%d requests_dispatched=%d", i+1, offered)
		}
	}
	wg.Wait()
	span := time.Since(start)
	r.log.Printf("traffic offered_rps=%d completed_rps=%.2f seconds=%.3f planned_requests=%d requests=%d statuses=%v", r.rate, float64(len(t.wire))/span.Seconds(), span.Seconds(), offered, len(t.wire), t.codes)
	r.log.Printf("latency_ms http %s; from_scheduled %s", percentiles(t.wire), percentiles(t.scheduled))
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if len(t.wire) < offered {
		return nil, fmt.Errorf("offered %d requests, measured %d", offered, len(t.wire))
	}
	return manifest, nil
}
