package renamedcaller

import "example.com/golden/renamed_dir"

// UseRenamed calls through the package's declared name, not its directory's.
func UseRenamed() int {
	return renamed.Do()
}
