package typed

// Set is generic: its methods are Set.Add, not Set[T].Add.
type Set[T comparable] struct{ items map[T]struct{} }

func (s *Set[T]) Add(v T) { s.items[v] = struct{}{} }

func (s *Set[T]) AddAll(vs []T) {
	for _, v := range vs {
		s.Add(v)
	}
}

func useSet() {
	var set Set[string]
	set.Add("a")
}
