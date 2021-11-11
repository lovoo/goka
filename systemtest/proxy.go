package systemtest

import (
	"net"
	"sync"
)

// FIProxy is a fault injecting proxy hooked into the sarama-config
// to proxy connections to kafka and inject connection loss etc.
type FIProxy struct {
	m sync.RWMutex

	readErr  error
	writeErr error

	conns map[string]*Conn
}

type Conn struct {
	net.Conn
	fip *FIProxy
}

func (c *Conn) Close() error {
	defer c.fip.removeConn(c.Conn)
	return c.Conn.Close()
}

func (c *Conn) Read(b []byte) (int, error) {
	c.fip.m.RLock()
	defer c.fip.m.RUnlock()

	if c.fip.readErr != nil {
		return 0, c.fip.readErr
	}
	return c.Conn.Read(b)
}

func (c *Conn) Write(b []byte) (int, error) {
	c.fip.m.RLock()
	defer c.fip.m.RUnlock()
	if c.fip.writeErr != nil {
		return 0, c.fip.writeErr
	}
	return c.Conn.Write(b)
}

func NewFIProxy() *FIProxy {
	return &FIProxy{
		conns: make(map[string]*Conn),
	}
}

func (fip *FIProxy) Dial(network, addr string) (c net.Conn, err error) {
	fip.m.Lock()
	defer fip.m.Unlock()

	conn, err := net.Dial(network, addr)

	wrappedConn := &Conn{
		Conn: conn,
		fip:  fip,
	}
	key := conn.LocalAddr().String()

	fip.conns[key] = wrappedConn
	return wrappedConn, err
}

func (fip *FIProxy) removeConn(c net.Conn) {
	fip.m.Lock()
	defer fip.m.Unlock()

	delete(fip.conns, c.LocalAddr().String())
}

func (fip *FIProxy) getConns() []string {
	fip.m.Lock()
	defer fip.m.Unlock()
	var conns []string

	for c := range fip.conns {
		conns = append(conns, c)
	}

	return conns
}

func (fip *FIProxy) SetReadError(err error) {
	fip.m.Lock()
	defer fip.m.Unlock()
	fip.readErr = err
}

func (fip *FIProxy) SetWriteError(err error) {
	fip.m.Lock()
	defer fip.m.Unlock()
	fip.writeErr = err
}

func (fip *FIProxy) ResetErrors() {
	fip.m.Lock()
	defer fip.m.Unlock()
	fip.readErr = nil
	fip.writeErr = nil
}

func (fip *FIProxy) String() string {
	return "Fault Injecting Proxy (FIP)"
}
