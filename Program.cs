using ABKR.Parser;
using ABKR.Utils;

try
{
    string inputFilePath = ConsoleInputHandler.GetSourceFileName();
    Parser parser = new(inputFileName);

    Console.WriteLine(parser.Parse(FileHandler.GetFileText(inputFileName)));
}
catch (Exception e)
{
    Console.WriteLine(e.Message);
}



String handleConsoleInput()
{

}