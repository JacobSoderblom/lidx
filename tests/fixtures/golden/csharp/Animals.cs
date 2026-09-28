namespace Golden.Animals
{
    public interface ISpeaker
    {
        string Say();
    }

    public class Animal
    {
        // Base method `Dog` inherits without overriding.
        public string Speak() => "...";
    }

    public class Dog : Animal
    {
        public string Bark() => "Woof";
    }

    // Decoy: an unrelated class with its own `Speak` and `Say`.
    public class Cat : ISpeaker
    {
        public string Speak() => "Meow";

        public string Say() => "Meow";
    }
}
