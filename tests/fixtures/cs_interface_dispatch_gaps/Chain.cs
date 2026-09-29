namespace Shop
{
    public interface IB
    {
        void Run(int id);
    }

    public interface IA : IB
    {
    }

    public class Worker : IA
    {
        public void Run(int id)
        {
        }
    }

    public class ChainCaller
    {
        private readonly IB _b;

        public ChainCaller(IB b)
        {
            _b = b;
        }

        public void Go()
        {
            _b.Run(1);
        }
    }
}
