using Golden.App;

namespace Golden.Downstream
{
    public class Downstream
    {
        // Cross-file incoming call into `Caller.Entry`.
        public string UseEntry(Caller c) => c.Entry();
    }
}
