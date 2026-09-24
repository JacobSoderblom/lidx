namespace Golden.ImplicitThis
{
    public class Derived : Base
    {
        // Unqualified call inside a class body: implicit `this.ImplicitHelper()`.
        // Before the fix, this was wrongly treated as a bare (receiverless)
        // call and refused by the bare-call guard -- but in C# an
        // unqualified call inside a class always has a receiver, just not
        // a written one (issue #75 follow-up, finding C).
        public string Run() => ImplicitHelper();
    }
}
