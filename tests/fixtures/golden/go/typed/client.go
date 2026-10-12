package typed

import (
	"bytes"
	"net"
)

type Client struct {
	conn net.Conn
	srv  *Server
	wrap Wrapper
	all  []*Server
}

// run closes a net.Conn, a standard-library type: external, never Report.Close.
func (c *Client) run() {
	c.conn.Close()
}

// viaField reaches Server.Handle through the field's declared type.
func (c *Client) viaField() string {
	return c.srv.Handle()
}

// viaEmbedded: Wrapper embeds Base, which declares Ping.
func (c *Client) viaEmbedded() {
	c.wrap.Ping()
}

// eachServer ranges over a slice-typed field.
func (c *Client) eachServer() {
	for _, s := range c.all {
		s.Handle()
	}
}

// viaConstructor types the local from NewServer's declared result.
func viaConstructor() string {
	s := NewServer()
	return s.Handle()
}

// viaChain types each link by the declared result of the one before.
func viaChain() string {
	return NewServer().Handle()
}

// literal and var forms.
func viaLiterals() {
	a := &Server{}
	b := Server{}
	var c Server
	var d *Server
	a.Handle()
	b.Handle()
	c.Handle()
	d.Handle()
}

// A standard-library value: no repo edge.
func viaBuffer() string {
	var b bytes.Buffer
	return b.String()
}

// A type assertion names the type; an interface-typed local names none.
func viaAssertion(x interface{}) {
	s := x.(*Server)
	s.Handle()
	x.Handle()
}

// A type switch binds the case type in its clause.
func viaSwitch(x interface{}) {
	switch v := x.(type) {
	case *Server:
		v.Handle()
	case *Report:
		v.String()
	}
}

var shared = NewServer()

// A package-level variable is typed from its initializer.
func viaGlobal() string {
	return shared.Handle()
}

// A parameter shadowing nothing, called inside a closure.
func viaClosure(s *Server) {
	func() {
		s.Handle()
	}()
}
