using System.Runtime.InteropServices;
using System.Text.RegularExpressions;

namespace Frontend.Components
{
    public class SqlRichTextBox : RichTextBox
    {
        private static readonly Color CommentColor = Color.Gray;
        private static readonly Color MultilineCommentColor = Color.Green;
        private static readonly Color KeywordColor = Color.Blue;
        private static readonly Color StringColor = Color.Brown;
        private static readonly Color NumberColor = Color.Magenta;
        private static readonly Color DateColor = Color.DarkCyan;
        private static readonly Color BoolColor = Color.Orange;

        private static readonly Regex CommentRegex = new(@"--.*$", RegexOptions.Multiline);
        private static readonly Regex MultilineCommentRegex = new(@"/\*.*?\*/", RegexOptions.Singleline);
        private static readonly Regex KeywordRegex = new(@"\b(SELECT|FROM|WHERE|INSERT|UPDATE|DELETE)\b", RegexOptions.IgnoreCase);
        private static readonly Regex StringRegex = new("\".*?\"|'.*?'", RegexOptions.Singleline);
        private static readonly Regex NumberRegex = new(@"\b\d+\b");
        private static readonly Regex DateRegex = new(@"'(\d{4}-\d{2}-\d{2})'");
        private static readonly Regex BoolRegex = new(@"\b(true|false)\b", RegexOptions.IgnoreCase);
        private RichTextBox _hiddenRtb = new();

        public SqlRichTextBox()
        {
        }

        public void Colorize()
        {
            int originalStart = SelectionStart;
            int originalLength = SelectionLength;

            LockWindowUpdate(Handle);

            int firstLine = GetLineFromCharIndex(originalStart);
            int lastLine = GetLineFromCharIndex(originalStart + originalLength);

            for (int line = firstLine; line <= lastLine; line++)
            {
                int lineStart = GetFirstCharIndexFromLine(line);
                int lineEnd = line < Lines.Length - 1 ? GetFirstCharIndexFromLine(line + 1) - 1 : TextLength;
                int lineLength = lineEnd - lineStart;

                Select(lineStart, lineLength);
                SelectionColor = ForeColor;

                ColorizeLine(CommentRegex, CommentColor);
                ColorizeLine(MultilineCommentRegex, MultilineCommentColor);
                ColorizeLine(KeywordRegex, KeywordColor);
                ColorizeLine(StringRegex, StringColor);
                ColorizeLine(NumberRegex, NumberColor);
                ColorizeLine(DateRegex, DateColor);
                ColorizeLine(BoolRegex, BoolColor);
            }

            Select(originalStart, originalLength);
            SelectionColor = ForeColor;

            LockWindowUpdate(IntPtr.Zero);
        }

        private void ColorizeLine(Regex regex, Color color)
        {
            foreach (Match match in regex.Matches(SelectedText).Cast<Match>())
            {
                Select(SelectionStart + match.Index, match.Length);
                SelectionColor = color;
            }
        }

        protected override void OnTextChanged(EventArgs e)
        {
            base.OnTextChanged(e);
            Colorize();
        }

        [DllImport("user32.dll")]
        private static extern bool LockWindowUpdate(IntPtr hWnd);
    }
}
