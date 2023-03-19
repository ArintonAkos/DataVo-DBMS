using Server.Logging;
using Server.Models;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.MongoDB;
using System.Text.RegularExpressions;

namespace Server.Parser.DDL
{
    internal class CreateTable : BaseDbAction
    {    
        public CreateTableModel Model { get; private set; }

        public CreateTable(Match match)
        {
            this.Model = CreateTableModel.FromMatch(match);
        }

        public override void PerformAction(string session)
        {
            try
            {
                Catalog.CreateTable(Model.ToTable(), "University");

                DbContext.Instance.CreateTable(Model.TableName, "University");

                Logger.Info($"New table {Model.TableName} successfully created!");
                Messages.Add($"Table {Model.TableName} successfully created!");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                Messages.Add(e.Message);
            }
        }
    }
}
