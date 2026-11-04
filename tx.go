package main

import (
	"context"

	"github.com/SimsChad12/go-redis/internal/pool"
)

// TxPipeline wraps Pipeline commands inside MULTI/EXEC transaction semantics.
type TxPipeline struct {
	Pipeline
}

func NewTxPipeline(connPool *pool.ConnPool) *TxPipeline {
	return &TxPipeline{
		Pipeline: *NewPipeline(connPool),
	}
}

// Exec executes all queued transaction commands and guarantees c.cmds is cleared on all exit paths,
// including context cancellation or deadline exceeded.
func (c *TxPipeline) Exec(ctx context.Context) ([]Cmder, error) {
	c.mu.Lock()
	if len(c.cmds) == 0 {
		c.cmds = nil
		c.mu.Unlock()
		return nil, nil
	}

	cmds := c.cmds
	c.cmds = nil // Clear early to prevent double execution/leak on early exit
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		if len(c.cmds) == 0 {
			c.cmds = nil
		}
		c.mu.Unlock()
	}()

	if err := ctx.Err(); err != nil {
		setCmdsErr(cmds, err)
		return cmds, err
	}

	// Wrap queued commands with MULTI/EXEC for transactional execution while returning user cmds
	txCmds := make([]Cmder, 0, len(cmds)+2)
	txCmds = append(txCmds, NewCmd("multi"))
	txCmds = append(txCmds, cmds...)
	txCmds = append(txCmds, NewCmd("exec"))

	err := c.exec(ctx, txCmds)
	if err != nil {
		setCmdsErr(cmds, err)
	}
	return cmds, err
}
