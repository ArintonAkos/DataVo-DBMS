
namespace Server.Logging
{
    internal class Logger
    {
        private static String _dateTimeFormat = "dd/MM/yy HH:mm:ss:fff";

        public static void Info(String message)
        {
            Console.WriteLine(GetTime() + message);
        }

        public static void Error(String message)
        {
            Console.WriteLine(GetTime() + "Error: " + message);
        }

        private static String GetTime()
        {
            DateTime dateTime = DateTime.Now;

            return "[" + dateTime.ToString(_dateTimeFormat) + "] ";
        }
    }
}
