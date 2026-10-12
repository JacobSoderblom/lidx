package vendoruser

import "example.com/thirdparty/lib"

// UseVendored calls a package vendored into the repository.
func UseVendored() int {
	return lib.Vendored()
}
