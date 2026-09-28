package caller

import (
	"fmt"

	"example.com/golden/ambiguous_a"
	"example.com/golden/animals"
	"example.com/golden/greeter"
	"example.com/golden/helper"
)

// localUtil is unexported: only callable within package caller.
func localUtil() string {
	return "local"
}

// Entry: a plain same-package call.
func Entry() string {
	return localUtil()
}

// CallSamePackageOtherFile calls an unexported function defined in a
// different file of the same package.
func CallSamePackageOtherFile() string {
	return siblingUtil()
}

// CallImportedHelper: a package-qualified call to an exported function.
func CallImportedHelper(name string) string {
	return helper.FormatGreeting(name)
}

// CallReceiverTyped: `g`'s declared type pins `.Greet`.
func CallReceiverTyped(g *greeter.Greeter) string {
	return g.Greet("world")
}

// CallInherited: `Dog` embeds `Animal` and has no `Speak` of its own.
func CallInherited(d *animals.Dog) string {
	return d.Speak()
}

// CallInterface: dispatch through the `Speaker` interface.
func CallInterface(s animals.Speaker) string {
	return s.Say()
}

// CallAmbiguous: `Run` exists in two packages; the import picks
// ambiguous_a.
func CallAmbiguous() {
	ambiguous_a.Run()
}

// CallExternal: `fmt` is never indexed.
func CallExternal() string {
	return fmt.Sprint("x")
}
