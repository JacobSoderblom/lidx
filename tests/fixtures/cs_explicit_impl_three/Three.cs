namespace Shop
{
    public interface IA { void Run(); }
    public interface IB { void Run(); }
    public interface IC { void Run(); }

    public class D : IA, IB, IC
    {
        void IA.Run() { }
        public void Run() { }
    }
}
