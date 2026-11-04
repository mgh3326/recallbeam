package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/mgh3326/recallbeam/migrations"
)

func TestKeepaliveContinuesAfterFailureAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := make(chan struct{}, 3)
	done := make(chan struct{})
	go func() {
		defer close(done)
		keepalive(ctx, time.Millisecond, func(context.Context) error {
			select {
			case calls <- struct{}{}:
			default:
			}
			return errors.New("offline")
		})
	}()
	for range 2 {
		select {
		case <-calls:
		case <-time.After(time.Second):
			t.Fatal("keepalive did not retry")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("keepalive failed to stop")
	}
}

func TestListenAddressDefaultsToLoopback(t *testing.T) {
	t.Setenv("RB_LISTEN_ADDR", "")
	addr := listenAddress()
	host, port, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || port != "8080" {
		t.Fatalf("unsafe default listener: %q (%v)", addr, err)
	}
}
func TestListenAddressPreservesOverride(t *testing.T) {
	t.Setenv("RB_LISTEN_ADDR", ":9090")
	if got := listenAddress(); got != ":9090" {
		t.Fatal(got)
	}
}

func TestGenerationIsStableAndCoversMigrations(t *testing.T) {
	if g := generation(); g <= 0 || g != generation() {
		t.Fatal("generation must be positive and stable across calls")
	}
	if migrations.Digest() == "" || migrations.Digest() != migrations.Digest() {
		t.Fatal("migration digest must be stable")
	}
}
