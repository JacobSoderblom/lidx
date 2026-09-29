using System;

namespace Shop
{
    public interface IA
    {
        int P { get; }
        event EventHandler Changed;
    }

    public interface IB
    {
        int P { get; }
    }

    public class C : IA, IB
    {
        int IA.P => 1;

        public int P => 2;

        event EventHandler IA.Changed
        {
            add { }
            remove { }
        }
    }

    public class D : IA
    {
        public int P => 3;

        public event EventHandler Changed;
    }
}
