package greeter

type Greeter struct{}

// Greet: a method call on the receiver.
func (g *Greeter) Greet(name string) string {
	return g.format(name)
}

func (g *Greeter) format(name string) string {
	return "Hello"
}

// LoudGreeter is a decoy with its own `Greet` and `format`.
type LoudGreeter struct{}

func (l *LoudGreeter) Greet(name string) string {
	return "HELLO"
}

func (l *LoudGreeter) format(name string) string {
	return "LOUD"
}
