package gocbcoreps

import (
	"sync/atomic"
)

type routingConnPool struct {
	conns []*routingConn

	idx  uint32
	size uint32
}

func newRoutingConnPool(conns []*routingConn) *routingConnPool {
	return &routingConnPool{
		conns: conns,
		size:  uint32(len(conns)),
	}
}

func (pool *routingConnPool) Conn() *routingConn {
	idx := atomic.AddUint32(&pool.idx, 1)
	return pool.conns[idx%pool.Size()]
}

func (pool *routingConnPool) Size() uint32 {
	return pool.size
}

func (pool *routingConnPool) Close() error {
	var err error
	for _, conn := range pool.conns {
		closeErr := conn.Close()
		if closeErr != nil {
			err = closeErr
		}
	}

	return err
}
