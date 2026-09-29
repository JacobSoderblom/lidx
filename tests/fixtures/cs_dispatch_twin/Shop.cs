namespace Shop
{
    public interface IA<T>
    {
        void Run();
    }

    public class C : IA<int>, IA<string>
    {
        public void Run()
        {
        }

        void IA<string>.Run()
        {
        }
    }

    public class D : IA<int>, IA<string>
    {
        void IA<int>.Run()
        {
        }

        void IA<string>.Run()
        {
        }

        public void Run()
        {
        }
    }
}
