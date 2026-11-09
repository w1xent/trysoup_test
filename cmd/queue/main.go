package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"soupqueue/internal/httpapi"
	"soupqueue/internal/queue"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: queue api|worker|health")
	}
	if os.Args[1] == "health" {
		client := &http.Client{Timeout: 2 * time.Second}
		r, err := client.Get("http://127.0.0.1:8080/health")
		if err != nil {
			log.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != 200 {
			log.Fatal(r.Status)
		}
		return
	}
	s, err := queue.Open(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer s.DB.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch os.Args[1] {
	case "api":
		server := &http.Server{Addr: ":8080", Handler: httpapi.Handler(s), ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
		stopped := make(chan struct{})
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdown); err != nil {
				log.Print(err)
			}
			close(stopped)
		}()
		log.Print("listening on :8080")
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
		<-stopped
	case "worker":
		queue.Work(ctx, s)
	default:
		log.Fatal("usage: queue api|worker|health")
	}
}
