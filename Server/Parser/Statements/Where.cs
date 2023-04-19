using Server.Models.Statement;

namespace Server.Parser.Statements
{
    internal class Where
    {
        private readonly WhereModel _model;

        public Where(string match)
        {
            _model = WhereModel.FromString(match);
        }

        public List<string> Evaluate(Dictionary<string, Dictionary<string, dynamic>> tableContents)
        {
            List<string> matchingRows = new();

            foreach (var rowContent in tableContents)
            {
                Node statementInstance = _model.Statement;

                if (StatementEvaluator.Evaluate(statementInstance, rowContent.Value))
                {
                    matchingRows.Add(rowContent.Key);
                }
            }

            return matchingRows;
        }
    }
}
