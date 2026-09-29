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
