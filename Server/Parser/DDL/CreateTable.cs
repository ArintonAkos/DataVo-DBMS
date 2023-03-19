using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.MongoDB;
using System.Text.RegularExpressions;

namespace Server.Parser.DDL
{
    internal class CreateTable : BaseDbAction
    {
        private readonly CreateTableModel _model;

        public CreateTable(Match match)
        {
            this._model = CreateTableModel.FromMatch(match);
        }

        public override void PerformAction(string session)
        {
            try
            {
                Catalog.CreateTable(_model.ToTable(), "University");

                DbContext.Instance.CreateTable(_model.TableName, "University");

                Logger.Info($"New table {_model.TableName} successfully created!");
                Messages.Add($"Table {_model.TableName} successfully created!");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                Messages.Add(e.Message);
            }
        }
    }
}
