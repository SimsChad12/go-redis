package main

import (
	"context"
	"sync"

	"github.com/SimsChad12/go-redis/internal/pool"
)

// Cmder represents a Redis command queued in a pipeline.
type Cmder interface {
	Name() string
	Args() []interface{}
	SetErr(err error)
	Err() error
}

// BaseCmd implements Cmder for standard Redis commands (Set, Get, etc.).
type BaseCmd struct {
	args []interface{}
	err  error
	val  string
}

func NewCmd(args ...interface{}) *BaseCmd {
	return &BaseCmd{args: args}
}

func (c *BaseCmd) Name() string {
	if len(c.args) == 0 {
		return ""
	}
	s, _ := c.args[0].(string)
	return s
}

func (c *BaseCmd) Args() []interface{} {
	return c.args
}

func (c *BaseCmd) SetErr(err error) {
	c.err = err
}

func (c *BaseCmd) Err() error {
	return c.err
}

func (c *BaseCmd) SetVal(v string) {
	c.val = v
}

func (c *BaseCmd) Val() string {
	return c.val
}

// ExecHook allows custom execution / network I/O simulation in tests.
type ExecHook func(ctx context.Context, cn *pool.Conn, cmds []Cmder) error

// Pipeline queues multiple Redis commands and executes them in a single batch.
type Pipeline struct {
	mu       sync.Mutex
	cmds     []Cmder
	connPool *pool.ConnPool
	execHook ExecHook
}

func NewPipeline(connPool *pool.ConnPool) *Pipeline {
	if connPool == nil {
		connPool = pool.NewConnPool()
	}
	return &Pipeline{
		connPool: connPool,
	}
}

func (c *Pipeline) SetExecHook(fn ExecHook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execHook = fn
}

func (c *Pipeline) Process(ctx context.Context, cmd Cmder) error {
	c.mu.Lock()
	c.cmds = append(c.cmds, cmd)
	c.mu.Unlock()
	return nil
}

func (c *Pipeline) Set(ctx context.Context, key string, val interface{}) *BaseCmd {
	cmd := NewCmd("set", key, val)
	_ = c.Process(ctx, cmd)
	return cmd
}

func (c *Pipeline) Get(ctx context.Context, key string) *BaseCmd {
	cmd := NewCmd("get", key)
	_ = c.Process(ctx, cmd)
	return cmd
}

func (c *Pipeline) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cmds)
}

func (c *Pipeline) Discard() {
	c.mu.Lock()
	for i := range c.cmds {
		c.cmds[i] = nil
	}
	c.cmds = nil
	c.mu.Unlock()
}

func setCmdsErr(cmds []Cmder, err error) {
	if err == nil {
		return
	}
	for _, cmd := range cmds {
		if cmd != nil && cmd.Err() == nil {
			cmd.SetErr(err)
		}
	}
}

// Exec executes all queued commands and guarantees c.cmds is cleared on all exit paths,
// including context cancellation or timeout.
func (c *Pipeline) Exec(ctx context.Context) ([]Cmder, error) {
	c.mu.Lock()
	if len(c.cmds) == 0 {
		c.cmds = nil
		c.mu.Unlock()
		return nil, nil
	}

	cmds := c.cmds
	c.cmds = nil // Clear early to prevent double execution or memory leak on early exit
	c.mu.Unlock()

	// Ensure c.cmds remains nil even if panic or concurrent state issue occurs
	defer func() {
		c.mu.Lock()
		if len(c.cmds) == 0 {
			c.cmds = nil
		}
		c.mu.Unlock()
	}()

	// Check context before execution
	if err := ctx.Err(); err != nil {
		setCmdsErr(cmds, err)
		return cmds, err
	}

	err := c.exec(ctx, cmds)
	return cmds, err
}

func (c *Pipeline) exec(ctx context.Context, cmds []Cmder) error {
	if err := ctx.Err(); err != nil {
		setCmdsErr(cmds, err)
		return err
	}

	cn, err := c.connPool.Get(ctx)
	if err != nil {
		setCmdsErr(cmds, err)
		return err
	}

	var execErr error
	if c.execHook != nil {
		execErr = c.execHook(ctx, cn, cmds)
	} else {
		// Default execution path checking context mid-stream
		select {
		case <-ctx.Done():
			execErr = ctx.Err()
		default:
			for _, cmd := range cmds {
				if err := ctx.Err(); err != nil {
					execErr = err
					break
				}
				if bc, ok := cmd.(*BaseCmd); ok {
					bc.SetVal("OK")
				}
			}
		}
	}

	if execErr != nil {
		setCmdsErr(cmds, execErr)
		// If context was cancelled or timed out during I/O, mark connection broken and remove from pool
		if ctx.Err() != nil {
			cn.MarkBroken()
			c.connPool.Remove(cn)
			return execErr
		}
	}

	c.connPool.Put(ctx, cn)
	return execErr
}
