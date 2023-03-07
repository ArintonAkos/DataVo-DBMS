using ABKR.Parser;
using ABKR.Utils;

try
{
    string inputFilePath = ConsoleInputHandler.GetSourceFileName();
    Parser parser = new(inputFilePath);

    Console.WriteLine(parser.Parse());
}
catch (Exception e)
{
    Console.WriteLine(e.Message);
}



String handleConsoleInput()
{
    return String.Empty;
}