package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"soupqueue/internal/bootstrap"
	"soupqueue/internal/config"
	"soupqueue/internal/httpapi"
	"soupqueue/internal/queue"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: queue api|worker|health|bootstrap|token")
	}
	if os.Args[1] == "bootstrap" {
		if err := bootstrap.Run("/bootstrap"); err != nil {
			log.Fatal(err)
		}
		return
	}
	if os.Args[1] == "token" {
		tokens, err := config.Tokens()
		if err != nil {
			log.Fatal(err)
		}
		if tokens["operator"] == "" {
			log.Fatal("operator token is not configured")
		}
		fmt.Println(tokens["operator"])
		return
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
	dsn, err := config.Value("DATABASE_URL")
	if err != nil {
		log.Fatal(err)
	}
	s, err := queue.Open(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer s.DB.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch os.Args[1] {
	case "api":
		tokens, err := config.Tokens()
		if err != nil {
			log.Fatal(err)
		}
		handler, err := httpapi.Handler(s, tokens)
		if err != nil {
			log.Fatal(err)
		}
		addr := os.Getenv("HTTP_ADDR")
		if addr == "" {
			addr = "127.0.0.1:8080"
		}
		server := &http.Server{Addr: addr, Handler: handler, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
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
		log.Printf("listening on %s", addr)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
		<-stopped
	case "worker":
		queue.Work(ctx, s)
	default:
		log.Fatal("usage: queue api|worker|health|bootstrap|token")
	}
}
