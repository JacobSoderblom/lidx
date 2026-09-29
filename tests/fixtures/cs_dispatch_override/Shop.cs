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
        }
    }
}
