using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DQL;
using Server.Parser.Actions;
using System.Text.RegularExpressions;

namespace Server.Parser.Commands
{
    internal class Describe : BaseDbAction
    {
        private readonly DescribeModel _model;

        public Describe(Match match)
        {
            _model = DescribeModel.FromMatch(match);
        }

        public override void PerformAction(Guid session)
        {
            try
            {
                Catalog.GetTableColumns(_model.TableName, "University")
                .ForEach(column => Fields.Add(new()
                {
                    FieldName = column.Name
                }));
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                Messages.Add(ex.Message);
            }
        }
    }
}
