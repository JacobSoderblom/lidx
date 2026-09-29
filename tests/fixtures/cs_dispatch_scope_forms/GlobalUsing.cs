global using Z;

namespace G
{
    public class GImpl : IX
    {
        public void Run()
        {
        }
    }

    public class GCaller
    {
        private readonly IX _x;

        public void Go()
        {
            _x.Run();
        }
    }
}
