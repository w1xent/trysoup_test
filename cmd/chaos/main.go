// Command chaos runs a disposable Compose project and records the entire run.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	mathrand "math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type run struct {
	project    string
	log        *log.Logger
	base       string
	seed       int64
	jobs, rate int
	chaos      bool
}

func (r *run) command(ctx context.Context, verbose bool, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Env = append(os.Environ(), "HTTP_PORT=0", "COMPOSE_PROFILES=")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if verbose || err != nil {
		r.log.Printf("$ %s\n%s%sresult=%v", strings.Join(args, " "), stdout.String(), stderr.String(), err)
	} else if stderr.Len() > 0 {
		r.log.Print(stderr.String())
	}
	if err != nil {
		return "", fmt.Errorf("%s: %w", args[0], err)
	}
	return strings.TrimSpace(stdout.String()), nil
}

func (r *run) compose(ctx context.Context, verbose bool, args ...string) (string, error) {
	return r.command(ctx, verbose, append([]string{"docker", "compose", "-p", r.project}, args...)...)
}

func (r *run) query(ctx context.Context, sql string) (string, error) {
	return r.compose(ctx, false, "exec", "-T", "db", "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "queue_admin", "-d", "queue", "-c", sql)
}

func (r *run) start(ctx context.Context) ([]string, error) {
	options := []string{"up", "--build", "-d", "--wait"}
	want := 4
	if r.chaos {
		want = 8
		options = append(options, "--scale", "worker=8")
	}
	if _, err := r.compose(ctx, true, options...); err != nil {
		return nil, err
	}
	addr, err := r.compose(ctx, false, "port", "api", "8080")
	if err != nil {
		return nil, err
	}
	r.base = "http://" + addr
	ids, err := r.compose(ctx, false, "ps", "-q", "worker")
	if err != nil {
		return nil, err
	}
	workers := strings.Fields(ids)
	if len(workers) != want {
		return nil, fmt.Errorf("expected %d workers, found %d", want, len(workers))
	}
	// Sort stable replica names so a seeded choice selects the same logical slot on replay.
	names, err := r.command(ctx, false, append([]string{"docker", "inspect", "-f", "{{.Name}}"}, workers...)...)
	if err != nil {
		return nil, err
	}
	workers = strings.Fields(strings.ReplaceAll(names, "/", ""))
	sort.Strings(workers)
	info, err := r.query(ctx, "SELECT version(), (SELECT count(*) FROM jobs), (SELECT count(*) FROM effects)")
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(info, "|0|0") {
		return nil, fmt.Errorf("expected a fresh database: %s", info)
	}
	r.log.Printf("api=%s workers=%v db=%s", r.base, workers, info)
	return workers, nil
}

func (r *run) kill(ctx context.Context, target string) error {
	if _, err := r.command(ctx, false, "docker", "kill", "--signal=KILL", target); err != nil {
		return err
	}
	status, err := r.command(ctx, false, "docker", "inspect", "-f", "{{.State.ExitCode}}", target)
	if err != nil {
		return err
	}
	if status != "137" {
		return fmt.Errorf("SIGKILL %s exit=%s", target, status)
	}
	if _, err = r.command(ctx, false, "docker", "start", target); err != nil {
		return err
	}
	r.log.Printf("SIGKILL victim=%s exit=%s restarted=true", target, status)
	return nil
}

func (r *run) drain(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()
	for {
		s, err := r.query(ctx, "SELECT count(*) FILTER (WHERE state='queued'), count(*) FILTER (WHERE state='running') FROM jobs")
		if err != nil {
			return err
		}
		queued, active, ok := strings.Cut(s, "|")
		if !ok {
			return fmt.Errorf("unexpected queue counts: %s", s)
		}
		r.log.Printf("drain queued=%s running=%s", queued, active)
		if queued == "0" && active == "0" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (r *run) exercise(ctx context.Context) error {
	workers, err := r.start(ctx)
	if err != nil {
		return err
	}
	workCtx, stop := context.WithCancelCause(ctx)
	type injection struct {
		kills int
		err   error
	}
	finished := make(chan injection, 1)
	go func() {
		var result injection
		defer func() { finished <- result }()
		if !r.chaos {
			return
		}
		random := mathrand.New(mathrand.NewSource(r.seed))
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-tick.C:
				if err := r.kill(ctx, workers[random.Intn(len(workers))]); err != nil {
					result.err = err
					stop(err)
					return
				}
				result.kills++
			}
		}
	}()
	traffic := newTraffic(r.base)
	token, err := r.compose(ctx, false, "exec", "-T", "api", "/queue", "token")
	if err != nil {
		stop(err)
		<-finished
		return err
	}
	traffic.token = token
	defer traffic.client.CloseIdleConnections()
	receipts, err := r.load(workCtx, traffic)
	if err == nil {
		err = r.drain(workCtx)
	}
	stop(err)
	faults := <-finished
	if faults.err != nil {
		return faults.err
	}
	if cause := context.Cause(workCtx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	if err != nil {
		return err
	}
	return r.verify(ctx, traffic, receipts, faults.kills)
}

func execute() int {
	seed := flag.Int64("seed", 4101, "seed for workload and victim selection")
	jobs := flag.Int("jobs", 10000, "number of unique jobs")
	rate := flag.Int("rps", 100, "offered HTTP requests per second")
	profile := flag.String("profile", "chaos", "chaos (8 workers, duplicates, cancellations, SIGKILL) or capacity (4 workers, new jobs)")
	flag.Parse()
	if *jobs <= 0 || *rate <= 0 || (*profile != "chaos" && *profile != "capacity") {
		flag.Usage()
		return 2
	}
	if _, err := os.Stat("compose.yaml"); err != nil {
		fmt.Fprintln(os.Stderr, "run from the repository root")
		return 2
	}
	if err := os.MkdirAll("evidence", 0755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	id := strings.ToLower(rand.Text()[:10])
	name := strings.Join([]string{time.Now().UTC().Format("20060102T150405Z"), *profile, "seed" + strconv.FormatInt(*seed, 10), id}, "-") + ".log"
	path := filepath.Join("evidence", name)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer f.Close()
	r := run{project: "soup-chaos-" + id, log: log.New(io.MultiWriter(f, os.Stdout), "", log.LUTC|log.Ldate|log.Lmicroseconds),
		seed: *seed, jobs: *jobs, rate: *rate, chaos: *profile == "chaos"}
	r.log.Printf("raw=%s profile=%s seed=%d unique=%d target_http_rps=%d project=%s", path, *profile, *seed, *jobs, *rate, r.project)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	err = r.exercise(ctx)
	cleanupCtx, finish := context.WithTimeout(context.Background(), 2*time.Minute)
	defer finish()
	if err != nil {
		r.log.Printf("test error: %v", err)
		_, _ = r.compose(cleanupCtx, true, "logs", "--no-color", "--tail", "80")
	}
	// The project and its volume were created just for this invocation.
	_, cleanupErr := r.compose(cleanupCtx, true, "down", "-v", "--rmi", "local", "--timeout", "5")
	if err != nil || cleanupErr != nil {
		r.log.Print("RESULT FAILED")
		return 1
	}
	r.log.Print("RESULT PASS")
	return 0
}

func main() {
	os.Exit(execute())
}
