package caller

import (
	"example.com/golden/validate_pkgb"
)

type Celsius float64

// UseValidate imports validate_pkgb only, so Validate is validate_pkgb's.
func UseValidate() bool {
	return validate_pkgb.Validate(1)
}

// Convert is a type conversion, not a call.
func Convert(x float64) Celsius {
	return Celsius(x)
}

// UseLocal calls a function of its own package declared in another file.
func UseLocal() bool {
	return sibling()
}
