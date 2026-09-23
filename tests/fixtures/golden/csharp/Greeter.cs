namespace Golden.Greeting
{
    public class Greeter
    {
        // Method call on `this`.
        public string Greet(string name) => this.Format(name);

        private string Format(string name) => "Hello";
    }

    // Decoy: a second, unrelated `Greet` and `Format`.
    public class LoudGreeter
    {
        public string Greet(string name) => "HELLO";

        private string Format(string name) => "LOUD";
    }
}
