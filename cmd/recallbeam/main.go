// Command recallbeam maintains a rebuildable source index and serves search.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mgh3326/recallbeam/internal/core"
	"github.com/mgh3326/recallbeam/internal/hk"
	"github.com/mgh3326/recallbeam/internal/oracle"
	"github.com/mgh3326/recallbeam/internal/transport"
	"github.com/mgh3326/recallbeam/migrations"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func required(names ...string) error {
	for _, n := range names {
		if os.Getenv(n) == "" {
			return fmt.Errorf("%s is required", n)
		}
	}
	return nil
}
func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: recallbeam version|migrate|serve|mcp|rebuild")
	}
	mode := os.Args[1]
	if mode == "version" || mode == "--version" {
		fmt.Println("recallbeam", version)
		return nil
	}
	if mode != "migrate" && mode != "serve" && mode != "mcp" && mode != "rebuild" {
		return errors.New("unknown command")
	}
	if err := required("RB_DB_HOST", "RB_DB_SERVICE", "RB_DB_USER", "RB_DB_PASSWORD", "RB_WALLET_DIR"); err != nil {
		return err
	}
	port := 1522
	if p := os.Getenv("RB_DB_PORT"); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid RB_DB_PORT")
		}
		port = n
	}
	db, err := oracle.Open(os.Getenv("RB_DB_HOST"), port, os.Getenv("RB_DB_SERVICE"), os.Getenv("RB_DB_USER"), os.Getenv("RB_DB_PASSWORD"), os.Getenv("RB_WALLET_DIR"))
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if mode == "migrate" {
		if err = migrations.Run(ctx, db); err != nil {
			return fmt.Errorf("migration failed; inspect database migration state: %w", err)
		}
		return nil
	}
	st := &oracle.Store{DB: db, Source: "hk"}
	keepCtx, stopKeep := context.WithCancel(ctx)
	keepDone := make(chan struct{})
	go func() { defer close(keepDone); keepalive(keepCtx, 12*time.Hour, st.Keepalive) }()
	defer func() { stopKeep(); <-keepDone }()
	if mode == "mcp" {
		return transport.MCP(ctx, os.Stdin, os.Stdout, st)
	}
	if err = required("RB_HK_URL", "RB_HK_TOKEN"); err != nil {
		return err
	}
	engine := &core.Engine{Store: st, Source: hk.Client{Base: os.Getenv("RB_HK_URL"), Token: os.Getenv("RB_HK_TOKEN")}, Name: "hk"}
	if mode == "rebuild" {
		// Attempt counts live in this process, so retry here: a row that keeps
		// failing is skipped on the last attempt instead of failing every run.
		// Only a held row or a schema reset is worth another pass; source and
		// store errors are returned at once.
		for i := 1; i <= core.RowAttempts; i++ {
			if err = engine.Tick(ctx, true); err == nil || ctx.Err() != nil || !(errors.Is(err, core.ErrRowHeld) || errors.Is(err, core.ErrReset)) {
				break
			}
		}
		if err != nil {
			return errors.New("rebuild failed; publication outcome may be unknown; rerun rebuild")
		}
		return nil
	}
	if err = required("RB_SEARCH_TOKEN"); err != nil {
		return err
	}
	interval := 15 * time.Second
	if p := os.Getenv("RB_SYNC_INTERVAL"); p != "" {
		interval, err = time.ParseDuration(p)
		if err != nil || interval < time.Second || interval > time.Hour {
			return errors.New("RB_SYNC_INTERVAL must be between 1s and 1h")
		}
	}
	addr := listenAddress()
	server := &http.Server{Addr: addr, Handler: transport.HTTP(st, os.Getenv("RB_SEARCH_TOKEN")), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 90 * time.Second, IdleTimeout: time.Minute}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); loop(workerCtx, engine, st, interval) }()
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	cancel()
	shutdownCtx, finish := context.WithTimeout(context.Background(), 10*time.Second)
	defer finish()
	_ = server.Shutdown(shutdownCtx)
	<-done
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.New("HTTP server failed")
	}
	return nil
}
func loop(ctx context.Context, e *core.Engine, st *oracle.Store, interval time.Duration) {
	poll := time.NewTicker(interval)
	defer poll.Stop()
	daily := time.NewTicker(24 * time.Hour)
	defer daily.Stop()
	sync := func(rebuild bool) {
		if err := e.Tick(ctx, rebuild); err != nil && ctx.Err() == nil {
			log.Print("index sync failed; retrying on next interval")
		}
	}
	sync(false)
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			sync(false)
		case <-daily.C:
			sync(true)
		}
	}
}

func keepalive(ctx context.Context, interval time.Duration, query func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := query(pingCtx); err != nil && ctx.Err() == nil {
				log.Print("database keepalive failed")
			}
			cancel()
		}
	}
}

func listenAddress() string {
	if addr := os.Getenv("RB_LISTEN_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:8080"
}
