package animals

type Speaker interface {
	Say() string
}

type Animal struct{}

// Speak is promoted to `Dog` through embedding.
func (a *Animal) Speak() string {
	return "..."
}

type Dog struct {
	Animal
}

func (d *Dog) Bark() string {
	return "Woof"
}

// Cat is a decoy with its own `Speak` and `Say`.
type Cat struct{}

func (c *Cat) Speak() string {
	return "Meow"
}

func (c *Cat) Say() string {
	return "Meow"
}
