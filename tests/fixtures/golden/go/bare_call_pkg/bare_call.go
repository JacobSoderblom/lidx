package bare_call_pkg

type Widget struct{}

// Process is exported and the only `Process` in this fixture -- a
// `method`-kind symbol. Before #75, a receiver-less call to the same name
// would fuzzy-bind to it via the bare-name fallback; #75 bars a bare call
// (no receiver at all) from binding to any `method`-kind candidate.
func (w *Widget) Process() string {
	return "widget"
}

// BareCaller calls Process with no receiver -- must stay UNRESOLVED.
func BareCaller() string {
	return Process()
}
