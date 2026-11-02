package main

import (
	"context"
	"errors"
	"testing"
	"time"
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
