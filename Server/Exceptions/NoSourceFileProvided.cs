namespace Server.Exceptions;

internal class NoSourceFileProvided : Exception
{
    public NoSourceFileProvided()
        : base("No source file was found when calling the compiler!")
    {
    }
}