
namespace Server.Exceptions
{
    internal class FileExtensionNotSupported : Exception
    {
        public FileExtensionNotSupported(string extension) 
            : base($"File extension not supported! Only *.{extension} files can be compiled!")
        {
        }
    }
}
