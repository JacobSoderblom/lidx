namespace SelfTest
{
    public class Program
    {
        public static int Run()
        {
            var calc = new Calculator();
            int x = calc.Add(1, 2);
            return calc.Multiply(x, 3);
        }

        public static int Again()
        {
            var calc = new Calculator();
            return calc.Add(4, 5);
        }
    }
}
