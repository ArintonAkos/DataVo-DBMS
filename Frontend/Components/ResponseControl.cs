using Frontend.Client.Responses;
using Frontend.Client.Responses.Controllers.Parser;
using Frontend.Client.Responses.Parts;

namespace Frontend.Components
{
    public partial class ResponseControl : UserControl
    {
        public ResponseControl()
        {
            InitializeComponent();
        }

        public void HandleResponse(ParseResponse response)
        {
            List<ActionResponse> tablesToRender = new();

            foreach (ScriptResponse scriptResponse in response.Data)
            {
                foreach (ActionResponse actionResponse in scriptResponse.Actions)
                {
                    if (actionResponse.Fields.Count > 0)
                    {
                        tablesToRender.Add(actionResponse);
                    }

                    actionResponse.Messages.ForEach(message => MessagesTextbox.Text += message + "\n");
                }
            }

            if (tablesToRender.Count > 0)
            {
                RenderTables(tablesToRender);
                tabControl.SelectedTab = OutputTabPanel;
                return;
            }

            tabControl.SelectedTab = MessagesTabPanel;
        }

        private void RenderTables(List<ActionResponse> tablesToRender)
        {
            OutputTabPanel.Controls.Clear();

            TableLayoutPanel tableLayout = new()
            {
                ColumnCount = 1,
                RowCount = tablesToRender.Count,
                Dock = DockStyle.Fill
            };

            foreach (ActionResponse table in tablesToRender)
            {
                tableLayout.RowStyles.Add(new RowStyle()
                {
                    SizeType = SizeType.Percent,
                    Height = 100 / tablesToRender.Count,
                });

                tableLayout.Controls.Add(RenderTable(table));
            }

            OutputTabPanel.Controls.Add(tableLayout);
        }

        private DataGridView RenderTable(ActionResponse table)
        {
            DataGridView result = new()
            {
                Dock = DockStyle.Fill,
                BackgroundColor = Color.White,
                BorderStyle = BorderStyle.None,
            };

            foreach (FieldResponse field in table.Fields)
            {
                result.Columns.Add(new DataGridViewTextBoxColumn()
                {
                    HeaderText = field.FieldName,
                    AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill
                });
            }

            foreach (var row in table.Data)
            {
                result.Rows.Add(row.ToArray());
            }

            return result;
        }
    }
}
