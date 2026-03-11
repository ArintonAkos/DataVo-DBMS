namespace DataVo.Core.Logging;

/// <summary>
/// Writes timestamped diagnostic messages to the console.
/// </summary>
/// <remarks>
/// The logger intentionally remains minimal so it can be used early in startup and recovery flows
/// without requiring an external logging framework.
/// </remarks>
public static class Logger
{
    /// <summary>
    /// Defines the timestamp format used for log prefixes.
    /// </summary>
    private static readonly string _dateTimeFormat = "dd/MM/yy HH:mm:ss:fff";

    /// <summary>
    /// Writes an informational message.
    /// </summary>
    /// <param name="message">The message to emit.</param>
    public static void Info(string message)
    {
        Console.WriteLine(GetTime() + message);
    }

    /// <summary>
    /// Writes an error message with the standard error prefix.
    /// </summary>
    /// <param name="message">The error message to emit.</param>
    public static void Error(string message)
    {
        Console.WriteLine(GetTime() + "Error: " + message);
    }

    /// <summary>
    /// Builds the timestamp prefix prepended to all log messages.
    /// </summary>
    /// <returns>The formatted timestamp prefix.</returns>
    private static string GetTime()
    {
        var dateTime = DateTime.Now;

        return "[" + dateTime.ToString(_dateTimeFormat) + "] ";
    }
}