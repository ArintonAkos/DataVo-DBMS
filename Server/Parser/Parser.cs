using ABKR.Utils;
using System;


namespace ABKR.Parser
{
    public class Parser
    {
        private String _filePath { get; set; }

        public Parser(String filePath)
        {
            this._filePath = filePath;
        }

        public String Parse()
        {
            String fileContent = FileHandler.GetFileText(this._filePath);

            return String.Empty;
        }
    }
}
