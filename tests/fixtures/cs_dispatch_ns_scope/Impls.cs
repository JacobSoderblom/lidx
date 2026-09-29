using N1;

namespace App
{
    public class C : IA, N2.IA
    {
        public void Run()
        {
        }

        void N2.IA.Run()
        {
        }
    }

    public class H : IA, N2.IA
    {
        void IA.Run()
        {
        }

        void N2.IA.Run()
        {
        }
    }

    public class OnlyN1 : IA
    {
        public void Run()
        {
        }
    }

    public class ViaN1
    {
        private readonly N1.IA _a;

        public void Go()
        {
            _a.Run();
        }
    }

    public class ViaN2
    {
        private readonly N2.IA _b;

        public void Go()
        {
            _b.Run();
        }
    }
}

namespace N2
{
    public class Local : IA
    {
        public void Run()
        {
        }
    }
}
