namespace Server.Logging;

internal class Logger
{
    private static readonly string _dateTimeFormat = "dd/MM/yy HH:mm:ss:fff";

    public static void Info(string message)
    {
        Console.WriteLine(GetTime() + message);
    }

    public static void Error(string message)
    {
        Console.WriteLine(GetTime() + "Error: " + message);
    }

    private static string GetTime()
    {
        var dateTime = DateTime.Now;

        return "[" + dateTime.ToString(_dateTimeFormat) + "] ";
    }
}