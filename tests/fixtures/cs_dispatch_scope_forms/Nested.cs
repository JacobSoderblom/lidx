namespace K
{
    public class Outer
    {
        public interface IX
        {
            void Run();
        }

        public class Impl : IX
        {
            public void Run()
            {
            }
        }

        private readonly IX _x;

        public void Go()
        {
            _x.Run();
        }
    }
}
