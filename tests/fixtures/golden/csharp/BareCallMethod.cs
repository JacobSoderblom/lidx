namespace Golden.BareCallMethod
{
    public class Utility
    {
        // The only `Recalc` in this fixture -- a method-kind symbol.
        // Before #75, a receiver-less call to the same name would
        // fuzzy-bind to this via the bare-name fallback; #75 bars a bare
        // call (no receiver at all) from binding to any method-kind
        // candidate.
        public string Recalc() => "widget";
    }

    public class BareCaller
    {
        // Bare call, no receiver: Recalc is not declared in this class
        // and there is no `using static` bringing it into scope. Must
        // stay UNRESOLVED.
        public string CallBare() => Recalc();
    }
}
