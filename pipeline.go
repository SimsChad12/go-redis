package redis

import (
	"context"
)

type Pipeline struct {
	cmds []Cmder
}

func (c *Pipeline) Len() int {
	return len(c.cmds)
}

func setCmdsErr(cmds []Cmder, err error) {
	for _, cmd := range cmds {
		cmd.SetErr(err)
	}
}

func (c *Pipeline) Exec(ctx context.Context) ([]Cmder, error) {
	if len(c.cmds) == 0 {
		return nil, nil
	}

	cmds := c.cmds
	c.cmds = nil

	if err := ctx.Err(); err != nil {
		setCmdsErr(cmds, err)
		return cmds, err
	}

	err := c.exec(ctx, cmds)
	if err != nil {
		setCmdsErr(cmds, err)
	}
	return cmds, err
}

func (c *Pipeline) exec(ctx context.Context, cmds []Cmder) error {
	// Stub for execution logic
	return nil
}
