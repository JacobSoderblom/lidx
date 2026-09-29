namespace Shop
{
    public abstract class Base
    {
        public abstract void M();

        public virtual void V()
        {
        }

        public void N()
        {
        }

        public abstract int P { get; }

        public virtual event System.EventHandler E;

        public virtual void Ov(int a)
        {
        }

        public virtual void Ov(string a, int b)
        {
        }
    }

    public class Derived : Base
    {
        public override void M()
        {
        }

        public override void V()
        {
        }

        public new void N()
        {
        }

        public override int P => 1;

        public override event System.EventHandler E;

        public override void Ov(int a)
        {
        }

        public new void Ov(string a, int b)
        {
        }
    }

    public class Mid : Base
    {
        public override void M()
        {
        }
    }

    public class Leaf : Mid
    {
        public override void M()
        {
        }
    }

    public class Caller
    {
        private readonly Base _b;

        public void Go()
        {
            _b.M();
            _b.V();
            _b.N();
            _b.Ov(1);
        }
    }
}
