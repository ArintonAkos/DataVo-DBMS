using MongoDB.Bson;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DML;
using Server.Parser.Actions;
using Server.Server.MongoDB;
using System.Text.RegularExpressions;

namespace Server.Parser.DML
{
    internal class DeleteFrom : BaseDbAction
    {
        private readonly DeleteFromModel _model;

        public DeleteFrom(Match match)
        {
            _model = DeleteFromModel.FromMatch(match);
        }

        public override void PerformAction(Guid session)
        {
            try
            {
                Dictionary<string, Dictionary<string, dynamic>> tableContents = 
                    DbContext.Instance.GetTableContents(_model.TableName, "University");
                
                List<string> toBeDeleted = _model.WhereStatement.Evaluate(tableContents);
                
                DbContext.Instance.DeleteFormTable(toBeDeleted, _model.TableName, "University");

                Logger.Info($"Rows affected: {toBeDeleted.Count}");
                Messages.Add($"Rows affected: {toBeDeleted.Count}");
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                Messages.Add(ex.Message);
            }
        }
    }
}
