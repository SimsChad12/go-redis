package main

import (
	"context"
	"fmt"
	"sync"
)

// Pipeline with context-aware Exec that prevents state inconsistency.
// The bug: context cancellation during Pipeline.Exec() leaves pipeline in inconsistent state.
// Fix: On context cancellation, mark pipeline as dirty and discard queued commands.
type Pipeline struct {
	mu      sync.Mutex
	cmds    []string
	dirty   bool
}

func NewPipeline() *Pipeline {
	return &Pipeline{}
}

func (p *Pipeline) Queue(cmd string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cmds = append(p.cmds, cmd)
}

func (p *Pipeline) Exec(ctx context.Context) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-ctx.Done():
		p.dirty = true
		p.cmds = nil
		return nil, ctx.Err()
	default:
	}

	results := p.cmds
	p.cmds = nil
	return results, nil
}

func main() {
	p := NewPipeline()
	p.Queue("SET key val")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.Exec(ctx)
	fmt.Printf("Exec with cancelled ctx: %v (expected canceled)\n", err)
}
