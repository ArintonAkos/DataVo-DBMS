namespace Server.Server.Http.Attributes;

[AttributeUsage(AttributeTargets.Method)]
internal class Method : Attribute
{
    public readonly string HttpMethod;

    public Method(string httpMethod)
    {
        HttpMethod = httpMethod;
    }
}