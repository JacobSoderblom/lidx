namespace Shop
{
    public interface IA
    {
        void Run();
    }

    public interface IB
    {
        void Run();
    }

    public class C : IA, IB
    {
        public void Run()
        {
        }

        void IA.Run()
        {
        }
    }

    public class Direct
    {
        public void Go(C c)
        {
            c.Run();
        }
    }

    public class ViaA
    {
        private readonly IA _a;

        public ViaA(IA a)
        {
            _a = a;
        }

        public void Go()
        {
            _a.Run();
        }
    }
}
