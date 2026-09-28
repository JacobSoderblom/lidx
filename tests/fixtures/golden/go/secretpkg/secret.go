package secretpkg

// secretUtil is unexported: only package secretpkg may call it. #75
// fixture (paired with package prober): a bare cross-package call to this
// must stay UNRESOLVED.
func secretUtil() string {
	return "secret"
}

// CallLocally calls secretUtil from within its own package --
// unaffected by the visibility guard (same package).
func CallLocally() string {
	return secretUtil()
}
