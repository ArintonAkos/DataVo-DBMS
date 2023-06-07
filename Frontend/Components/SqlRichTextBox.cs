using System.ServiceModel;
using System.Text.RegularExpressions;

namespace Frontend.Components
{
    public class SqlRichTextBox : RichTextBox
    {
        private readonly Regex Keywords = new(@"(SELECT|FROM|WHERE|JOIN|ON|GROUP|BY|INSERT|UPDATE|DELETE|CREATE|DROP|USE|GO|SHOW|TABLE|TABLES|DATABASE|DATABASES)");
        private readonly Color KeywordColor = Color.Blue;

        public SqlRichTextBox()
        {
        }

        public void ApplyHighlighting()
        {
            int currCaretPos = SelectionStart;
            int currSelectionLength = SelectionLength;

            foreach (Match match in Keywords.Matches(Text))
            {
                Select(match.Index, match.Length);
                SelectionColor = KeywordColor;
            }

            Select(currCaretPos, currSelectionLength);
            SelectionColor = Color.Black;
        }

        protected override void OnGotFocus(EventArgs e)
        {
            base.OnGotFocus(e);
            ApplyHighlighting();
        }
    }
}
