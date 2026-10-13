package main

type Command struct {
	Use  string
	RunE func() error
}

type Server struct{ routes map[string]func() }

func (s *Server) handle() {}

func (s *Server) Static() {}

func (s *Server) wire() {
	register(s.handle)   // method value passed as an argument
	_ = (*Server).Static // method expression
}

type Speaker interface{ Speak() }

func run() error { return nil }

var rootCmd = &Command{Use: "root", RunE: run} // struct-literal field handler, package level

var table = map[string]func(){"a": a} // map value, package level

func a()    {}
func b()    {}
func both() {}

func register(f func()) { f() }

func Map[T any](x T, f func(T) T) T { return f(x) }

func ident[T any](x T) T { return x }

func main() {
	cmd := &Command{Use: "x", RunE: run}
	_ = cmd
	register(b)
	both()         // called ...
	register(both) // ... and passed: the pair is a call too
	_ = Map(1, ident[int])
	var sp Speaker
	f := sp.Speak // interface method value
	_ = f
	g := (*Server).handle // method expression
	_ = g
	defer a() // a call
	go b()    // a call
	func() { register(a) }()
}
