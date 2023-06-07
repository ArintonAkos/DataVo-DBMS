using System.Text.RegularExpressions;

namespace Frontend.Components
{
    public class SqlRichTextBox : RichTextBox
    {
        private readonly string[] Keywords = { "SELECT", "FROM", "WHERE", "JOIN", "ON", "GROUP", "BY", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "USE", "GO", "SHOW", "TABLE", "TABLES", "DATABASE", "DATABASES" };
        private readonly Color KeywordColor = Color.Blue;

        public SqlRichTextBox()
        {
        }

        private void ColorizeLine(string line)
        {
            string[] tokens = new Regex("([ \\t();])").Split(line);
            foreach (string token in tokens)
            {
                SelectionColor = Color.Black;
                
                if (Keywords.Contains(token))
                {
                    SelectionColor = KeywordColor;
                }
                
                SelectedText = token;
            }
        }

        public void ApplyHighlighting()
        {
            int currCaretPos = SelectionStart;
            string[] lines = Text.Split('\n');

            Text = "";

            for (int i = 0; i < lines.Count(); ++i)
            {
                ColorizeLine(lines[i]);
                
                if (i < lines.Count() - 1)
                {
                    SelectedText = "\n";
                }
            }

            SelectedText = "";
            SelectionStart = currCaretPos;
        }

        protected override void OnTextChanged(EventArgs e)
        {
            base.OnTextChanged(e);
            ApplyHighlighting();
        }
    }
}
