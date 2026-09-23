package helper

// FormatGreeting is exported and called from package caller.
func FormatGreeting(name string) string {
	return decorate(name)
}

// decorate is unexported: only package helper may call it.
func decorate(name string) string {
	return "Hi"
}
