package typed

// Server is the receiver the typed calls bind to.
type Server struct{ Name string }

func NewServer() *Server { return &Server{} }

func (s *Server) Handle() string { return s.Name }

// Report is a decoy: it has a `String` and a `Close` of its own, which a
// call on a standard-library value must never bind to.
type Report struct{}

func (r *Report) String() string { return "report" }

func (r *Report) Close() error { return nil }

// Base is embedded into Wrapper, so Wrapper gets Ping.
type Base struct{}

func (b *Base) Ping() {}

type Wrapper struct {
	Base
	srv *Server
}
