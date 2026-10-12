package tools

import "example.com/golden/helper"

// UseHelper imports a package of the root module from a nested module.
func UseHelper() string {
	return helper.FormatGreeting("x")
}
