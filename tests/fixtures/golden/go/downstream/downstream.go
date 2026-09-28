package downstream

import "example.com/golden/caller"

// UseEntry: a cross-package incoming call into `caller.Entry`.
func UseEntry() string {
	return caller.Entry()
}
