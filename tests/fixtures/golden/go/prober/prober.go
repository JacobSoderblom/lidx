package prober

// CallRemotely calls secretUtil, which exists only in package secretpkg
// as an unexported function -- no import brings it into scope here, so
// this is a bare, unqualified call. Before #75, the bare-name fallback
// found exactly one same-language candidate in the whole fixture (the
// only secretUtil) and bound to it despite being unexported and declared
// in a different package. #75's visibility guard (capitalization +
// package-directory comparison) must refuse that cross-package bind.
func CallRemotely() string {
	return secretUtil()
}
