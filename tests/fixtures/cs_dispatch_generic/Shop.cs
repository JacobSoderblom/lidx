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

    public class Z : IB<string>
    {
        void IB<string>.Run()
        {
        }
    }

    public class ZCaller
    {
        private readonly IB<int> _b;

        public void Go()
        {
            _b.Run();
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

    public class StrCaller
    {
        public void Go(IA<string> s)
        {
            s.Run();
        }
    }

    public class CastCaller
    {
        public void Go(C c)
        {
            var x = (IA<int>)c;
            x.Run();
        }
    }

    public class OpenCaller<T>
    {
        private readonly IA<T> _o;

        public void Go()
        {
            _o.Run();
        }
    }

    public class TwoArgs
    {
        public void Go(IA<int> a, IA<string> b)
        {
            a.Run();
            b.Run();
        }
    }

    public class TwoArgsRev
    {
        public void Go(IA<string> b, IA<int> a)
        {
            b.Run();
            a.Run();
        }
    }
}
