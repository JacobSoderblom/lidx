using Alias = Z;

namespace H
{
    public class HCaller
    {
        private readonly Alias.IX _x;

        public void Go()
        {
            _x.Run();
        }
    }
}
