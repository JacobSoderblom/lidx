namespace SelfTest
{
    public class Calculator
    {
        public int Add(int a, int b)
        {
            return a + b;
        }

        public int Multiply(int a, int b)
        {
            return Add(a, 0) * b;
        }

        public int Unused(int a)
        {
            return a - 1;
        }
    }
}
