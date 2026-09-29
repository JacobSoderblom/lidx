namespace Shop
{
    public interface IA<T>
    {
        void Run();
    }

    public interface IB<T>
    {
        void Run();
    }

    public class C : IA<int>, IA<string>
    {
        void IA<int>.Run()
        {
        }

        void IA<string>.Run()
        {
        }
    }

    public class E : IA<int>, IB<int>
    {
        void IA<int>.Run()
        {
        }

        public void Run()
        {
        }
    }

    public class Caller
    {
        private readonly IA<int> _a;

        public void Go()
        {
            _a.Run();
        }
    }
}
