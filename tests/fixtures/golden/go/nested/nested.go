package nested

import "testing"

type Builder struct{}

func Helper() int { return 1 }

func Inner(x int) int { return x }

func Outer(x int) int { return x }

func A() *Builder { return &Builder{} }

func (b *Builder) B() *Builder { return b }

func (b *Builder) C() int { return Helper() }

// Nested calls as arguments: both Outer and Inner are called by NestedArgs;
// the builtin len emits nothing.
func NestedArgs() int {
	x := []int{1}
	return Outer(Inner(len(x)))
}

// A chain: A is called by the caller; B and C are method calls on a call
// result and stay unresolved (typing a receiver is a separate concern).
func Chain() int {
	return A().B().C()
}

func GoClosure() {
	go func() {
		Helper()
	}()
}

func DeferClosure() {
	defer func() {
		Helper()
	}()
}

func TestRun(t *testing.T) {
	t.Run("n", func(t *testing.T) {
		Helper()
	})
}

var hooks = []func(){
	func() {
		Helper()
	},
}

// A conversion and builtins are no calls.
func Conversions(b []byte) string {
	n := len(b)
	_ = n
	return string(b)
}
