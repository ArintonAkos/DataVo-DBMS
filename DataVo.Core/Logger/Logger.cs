namespace DataVo.Core.Logging;

/// <summary>
/// Writes timestamped diagnostic messages to the console when enabled.
/// </summary>
/// <remarks>
/// The logger intentionally remains minimal so it can be used early in startup and recovery flows
/// without requiring an external logging framework. It is off by default: an embedded database must not
/// write to its host application's console, and formatting a timestamp plus a synchronized console write
/// per message cost about a fifth of a SQL point read. Enable it with <see cref="IsEnabled"/> or the
/// <c>DATAVO_LOG</c> environment variable (any value other than <c>0</c>/<c>false</c>).
/// </remarks>
public static class Logger
{
    /// <summary>
    /// Defines the timestamp format used for log prefixes.
    /// </summary>
    private static readonly string _dateTimeFormat = "dd/MM/yy HH:mm:ss:fff";

    /// <summary>Gets or sets whether messages are written. Defaults to the <c>DATAVO_LOG</c> environment variable.</summary>
    public static bool IsEnabled { get; set; } = ReadEnvironmentSwitch();

    /// <summary>
    /// Writes an informational message.
    /// </summary>
    /// <param name="message">The message to emit.</param>
    public static void Info(string message)
    {
        if (!IsEnabled)
        {
            return;
        }

        Console.WriteLine(GetTime() + message);
    }

    /// <summary>
    /// Writes an error message with the standard error prefix.
    /// </summary>
    /// <param name="message">The error message to emit.</param>
    public static void Error(string message)
    {
        if (!IsEnabled)
        {
            return;
        }

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

    private static bool ReadEnvironmentSwitch()
    {
        string? value = Environment.GetEnvironmentVariable("DATAVO_LOG");
        return !string.IsNullOrEmpty(value)
            && !string.Equals(value, "0", StringComparison.Ordinal)
            && !string.Equals(value, "false", StringComparison.OrdinalIgnoreCase);
    }
}
