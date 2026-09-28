using System;
using Golden.Ambiguous.A;
using Golden.Animals;
using Golden.Greeting;
using Golden.Helpers;

namespace Golden.App
{
    public class Caller
    {
        private static string LocalUtil() => "local";

        // Plain call: a same-class static method.
        public string Entry() => LocalUtil();

        // `using` call: `Helper` is bound through `using Golden.Helpers`.
        public string CallImportedHelper(string name) => Helper.FormatGreeting(name);

        // Namespace path call: fully qualified, no `using` needed.
        public string CallNamespacePath(string name) => Golden.Helpers.Helper.FormatGreeting(name);

        // Receiver-typed call: `g`'s declared type pins `.Greet`.
        public string CallReceiverTyped(Greeter g) => g.Greet("world");

        // Inherited method call: `Dog` has no `Speak` of its own.
        public string CallInherited(Dog d) => d.Speak();

        // Interface call: dispatch through `ISpeaker`.
        public string CallInterface(ISpeaker s) => s.Say();

        // Ambiguous class name: `Runner` exists in two namespaces; the
        // `using Golden.Ambiguous.A` above picks A's.
        public void CallAmbiguous(Runner r) => r.Run();

        // External call: `Console` is never indexed.
        public void CallExternal() => Console.WriteLine("x");
    }
}
