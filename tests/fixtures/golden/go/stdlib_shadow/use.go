package shadow

import (
	"path"
	"path/filepath"
)

// JoinAll calls the standard library's Join twice, never the repo's.
func JoinAll() string {
	return path.Join("a", "b") + filepath.Join("c", "d")
}

// JoinLocal calls the repo's own Join: a bare name is the package's.
func JoinLocal() string {
	return Join("a")
}
