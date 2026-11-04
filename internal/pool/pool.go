package pool

import (
	"context"
	"errors"
	"sync"
)

var ErrClosed = errors.New("redis: connection pool is closed")

// Conn represents a pooled network connection to Redis.
type Conn struct {
	mu     sync.Mutex
	broken bool
	closed bool
	inUse  bool
}

func NewConn() *Conn {
	return &Conn{}
}

func (cn *Conn) MarkBroken() {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.broken = true
}

func (cn *Conn) IsBroken() bool {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	return cn.broken || cn.closed
}

func (cn *Conn) Close() error {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.closed = true
	cn.broken = true
	return nil
}

// ConnPool manages a pool of Conn instances and discards unhealthy/cancelled connections.
type ConnPool struct {
	mu          sync.Mutex
	idleConns   []*Conn
	closed      bool
	removedBad  int
	returnedOk  int
}

func NewConnPool() *ConnPool {
	return &ConnPool{
		idleConns: make([]*Conn, 0, 8),
	}
}

func (p *ConnPool) Get(ctx context.Context) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	for len(p.idleConns) > 0 {
		 idx := len(p.idleConns) - 1
		 cn := p.idleConns[idx]
		 p.idleConns = p.idleConns[:idx]
		 if !cn.IsBroken() {
			 cn.inUse = true
			 return cn, nil
		 }
	}
	cn := NewConn()
	cn.inUse = true
	return cn, nil
}

// Put returns a connection to the idle pool only if healthy; broken connections are closed.
func (p *ConnPool) Put(ctx context.Context, cn *Conn) {
	if cn == nil {
		return
	}
	if ctx != nil && ctx.Err() != nil {
		cn.MarkBroken()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	cn.inUse = false
	if p.closed || cn.IsBroken() {
		_ = cn.Close()
		p.removedBad++
		return
	}
	p.idleConns = append(p.idleConns, cn)
	p.returnedOk++
}

func (p *ConnPool) Remove(cn *Conn) {
	if cn == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	cn.inUse = false
	_ = cn.Close()
	p.removedBad++
}

func (p *ConnPool) Stats() (idle int, returnedOk int, removedBad int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idleConns), p.returnedOk, p.removedBad
}
