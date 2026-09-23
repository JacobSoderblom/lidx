namespace Golden.Helpers
{
    public static class Helper
    {
        public static string FormatGreeting(string name) => Decorate(name);

        private static string Decorate(string name) => "Hi";
    }
}
