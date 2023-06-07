using System.Text.RegularExpressions;

namespace Frontend.Components
{
    public class SqlRichTextBox : RichTextBox
    {
        private readonly Regex Keywords = new(@"(^|\s+)(SELECT|FROM|WHERE|AND|OR|JOIN|ON|GROUP\s+BY|INSERT\s+INTO|VALUES|UPDATE|DELETE\s+FROM|CREATE\s+TABLE|DROP\s+TABLE|USE|GO|SHOW\s+TABLES|CREATE\s+DATABASE|DROP\s+DATABASE|SHOW\s+DATABASES|'[^']+')($|\s+)", RegexOptions.IgnoreCase);
        private readonly Regex StringRegex = new(@"(^|\s+)('[^']+')($|\s+)", RegexOptions.IgnoreCase);
        private readonly Regex CommentRegex = new(@"--.*$", RegexOptions.Multiline);
        private readonly Regex MultilineCommentRegex = new(@"/\*.*?\*/", RegexOptions.Singleline);

        private readonly Color KeywordColor = Color.Blue;
        private readonly Color StringColor = Color.Magenta;
        private readonly Color CommentColor = Color.Gray;
        private readonly Color MultilineCommentColor = Color.Green;

        public SqlRichTextBox()
        {
        }

        public void ApplyHighlighting()
        {
            int currCaretPos = SelectionStart;
            int currSelectionLength = SelectionLength;

            Select(0, Text.Length);
            SelectionColor = Color.Black;

            foreach (Match match in Keywords.Matches(Text))
            {
                Select(match.Index, match.Length);
                SelectionColor = KeywordColor;
            }

            foreach (Match match in StringRegex.Matches(Text))
            {
                Select(match.Index, match.Length);
                SelectionColor = StringColor;
            }

            foreach (Match match in CommentRegex.Matches(Text))
            {
                Select(match.Index, match.Length);
                SelectionColor = CommentColor;
            }

            foreach (Match match in MultilineCommentRegex.Matches(Text))
            {
                Select(match.Index, match.Length);
                SelectionColor = MultilineCommentColor;
            }

            Select(currCaretPos, currSelectionLength);
            SelectionColor = Color.Black;
        }
    }
}
