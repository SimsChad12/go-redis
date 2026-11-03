package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SimsChad12/go-redis/internal/pool"
)

func TestPipelineExec_ContextCancellation(t *testing.T) {
	p := NewPipeline(nil)
	ctx := context.Background()

	setCmd := p.Set(ctx, "key1", "val1")
	getCmd := p.Get(ctx, "key1")

	if p.Len() != 2 {
		t.Fatalf("expected pipeline length 2, got %d", p.Len())
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // Pre-cancel context

	cmds, err := p.Exec(cancelCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if len(cmds) != 2 {
		t.Fatalf("expected 2 returned commands, got %d", len(cmds))
	}
	for i, cmd := range cmds {
		if !errors.Is(cmd.Err(), context.Canceled) {
			t.Fatalf("cmd[%d] expected context.Canceled, got %v", i, cmd.Err())
		}
	}
	if !errors.Is(setCmd.Err(), context.Canceled) || !errors.Is(getCmd.Err(), context.Canceled) {
		t.Fatalf("individual command references did not receive context.Canceled")
	}
	if p.Len() != 0 {
		t.Fatalf("expected pipeline length 0 after cancelled Exec, got %d", p.Len())
	}
}

func TestPipelineExec_ReuseAfterCancelledContext(t *testing.T) {
	p := NewPipeline(nil)
	bg := context.Background()

	// 1. Queue initial commands and execute with cancelled context
	p.Set(bg, "stale_key_1", "stale_val_1")
	p.Set(bg, "stale_key_2", "stale_val_2")

	cancelCtx, cancel := context.WithCancel(bg)
	cancel()

	_, err := p.Exec(cancelCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on first run, got %v", err)
	}
	if p.Len() != 0 {
		t.Fatalf("expected empty pipeline after cancelled run, got %d", p.Len())
	}

	// 2. Reuse the same pipeline with valid context and verify only new commands execute
	freshGet := p.Get(bg, "fresh_key")
	cmds, err := p.Exec(bg)
	if err != nil {
		t.Fatalf("expected nil error on reuse, got %v", err)
	}
	if len(cmds) != 1 {
		t.Fatalf("expected exactly 1 command executed on reuse, got %d", len(cmds))
	}
	if cmds[0] != freshGet {
		t.Fatalf("expected freshGet command, got %v", cmds[0])
	}
	if freshGet.Err() != nil {
		t.Fatalf("expected nil cmd error, got %v", freshGet.Err())
	}
	if p.Len() != 0 {
		t.Fatalf("expected pipeline length 0 after successful Exec, got %d", p.Len())
	}
}

func TestPipelineExec_MidNetworkCancellationDiscardsConnection(t *testing.T) {
	connPool := pool.NewConnPool()
	p := NewPipeline(connPool)

	cancelCtx, cancel := context.WithCancel(context.Background())
	p.SetExecHook(func(ctx context.Context, cn *pool.Conn, cmds []Cmder) error {
		// Simulate context cancellation occurring mid-write / mid-read
		cancel()
		return ctx.Err()
	})

	p.Set(context.Background(), "k", "v")
	cmds, err := p.Exec(cancelCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled during I/O, got %v", err)
	}
	if len(cmds) != 1 || !errors.Is(cmds[0].Err(), context.Canceled) {
		t.Fatalf("expected command error to be context.Canceled")
	}

	idle, returnedOk, removedBad := connPool.Stats()
	if idle != 0 || returnedOk != 0 || removedBad != 1 {
		t.Fatalf("expected unhealthy connection to be removed (idle=0, returnedOk=0, removedBad=1), got idle=%d returnedOk=%d removedBad=%d", idle, returnedOk, removedBad)
	}
}

func TestTxPipelineExec_DeadlineExceededAndReuse(t *testing.T) {
	tx := NewTxPipeline(nil)
	bg := context.Background()

	tx.Set(bg, "tx_key_1", "1")
	tx.Get(bg, "tx_key_1")

	timeoutCtx, cancel := context.WithTimeout(bg, 1*time.Nanosecond)
	time.Sleep(2 * time.Millisecond)
	defer cancel()

	cmds, err := tx.Exec(timeoutCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if len(cmds) != 2 {
		t.Fatalf("expected 2 commands returned from TxPipeline.Exec, got %d", len(cmds))
	}
	for _, cmd := range cmds {
		if !errors.Is(cmd.Err(), context.DeadlineExceeded) {
			t.Fatalf("expected command err context.DeadlineExceeded, got %v", cmd.Err())
		}
	}
	if tx.Len() != 0 {
		t.Fatalf("expected TxPipeline length 0 after timeout, got %d", tx.Len())
	}

	// Reuse TxPipeline with valid context
	tx.Set(bg, "tx_key_clean", "2")
	cleanCmds, err := tx.Exec(bg)
	if err != nil {
		t.Fatalf("expected nil error on TxPipeline reuse, got %v", err)
	}
	if len(cleanCmds) != 1 {
		t.Fatalf("expected 1 command on TxPipeline reuse, got %d", len(cleanCmds))
	}
}
