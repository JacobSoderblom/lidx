namespace F1;

public interface IX
{
    void Run();
}

public class FImpl : IX
{
    public void Run()
    {
    }
}

public class FCaller
{
    private readonly IX _x;

    public void Go()
    {
        _x.Run();
    }
}
