package typed_user

import (
	"example.com/golden/typed"
)

// A constructor of another package types the local.
func UseImported() string {
	s := typed.NewServer()
	return s.Handle()
}

// A parameter of another package's type.
func UseParam(s *typed.Server) string {
	return s.Handle()
}

// A variable declared with another package's type.
func UseVar() string {
	var s typed.Server
	return s.Handle()
}
