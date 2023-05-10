namespace Server.Server.Http.Attributes;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method)]
public class Route : Attribute
{
    public string Path;

    public Route(string path)
    {
        Path = path;
    }
}