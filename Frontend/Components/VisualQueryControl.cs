using Frontend.Client.Responses.Parts;
using Frontend.Services;

namespace Frontend.Components
{
    public partial class VisualQueryControl : UserControl
    {
        private bool isDragging = false;
        private Point lastMousePosition;
        private List<DatabaseModel> Databases = new();

        public VisualQueryControl()
        {
            InitializeComponent();
            InitData();

            Name = "VQueryEditor";
        }

        private async void InitData()
        {
            await FetchDatabases();
            CreateTableModels();
            CreateDatabaseTreeViews();
        }

        private void CreateDatabaseTreeViews()
        {
            foreach (var database in Databases)
            {
                if (database == null) continue;

                CollapsibleTreeNode databaseNode = new(database.DatabaseName);

                foreach (var table in database.Tables)
                {
                    CollapsibleTreeNode tableNode = new(table.Name);

                    tableNode.Nodes.Add(CreateForeignKeyNodes(table));
                    tableNode.Nodes.Add(CreateUniqueKeyNodes(table));
                    tableNode.Nodes.Add(CreateIndexFileNodes(table));

                    databaseNode.Nodes.Add(tableNode);
                }

                collapsibleTreeView1.Nodes.Add(databaseNode);
            }
        }

        private static CollapsibleTreeNode CreateIndexFileNodes(DatabaseModel.Table table)
        {
            CollapsibleTreeNode rootIndexFilesNode = new("Index Files");

            foreach (var indexFile in table.IndexFiles)
            {
                CollapsibleTreeNode indexFileNode = new(indexFile.IndexFileName);
                rootIndexFilesNode.Nodes.Add(indexFileNode);
            }

            return rootIndexFilesNode;
        }

        private static CollapsibleTreeNode CreateForeignKeyNodes(DatabaseModel.Table table)
        {
            CollapsibleTreeNode rootForeignKeyNode = new("Foreign Keys");

            foreach (var foreignKey in table.ForeignKeys)
            {
                CollapsibleTreeNode foreignKeyNode = new(foreignKey.AttributeName);
                rootForeignKeyNode.Nodes.Add(foreignKeyNode);
            }

            return rootForeignKeyNode;
        }

        private static CollapsibleTreeNode CreateUniqueKeyNodes(DatabaseModel.Table table)
        {
            CollapsibleTreeNode rootUniqueKeyNode = new("Unique Keys");

            foreach (var uniqueKey in table.UniqueKeys)
            {
                CollapsibleTreeNode uniqueKeyNode = new(uniqueKey);
                rootUniqueKeyNode.Nodes.Add(uniqueKeyNode);
            }

            return rootUniqueKeyNode;
        }

        private async Task FetchDatabases()
        {
            var response = await DatabaseService.GetDatabaseList();

            if (this.InvokeRequired)
            {
                this.Invoke((MethodInvoker)delegate
                {
                    Databases = response.Data;
                });
            }
            else
            {
                Databases = response.Data;
            }
        }

        private void CreateTableModels()
        {
            DatabaseModel database = Databases[0];
            int offset = 10;
            int topPosition = 0;

            foreach (DatabaseModel.Table table in database.Tables)
            {
                var panel = new DraggablePanel(table)
                {
                    Left = topPosition
                };
                panel.Dock = DockStyle.Top;

                if (this.InvokeRequired)
                {
                    this.Invoke((MethodInvoker)delegate
                    {
                        TableVisualizerPanel.Controls.Add(panel);
                    });
                }
                else
                {
                    TableVisualizerPanel.Controls.Add(panel);
                }

                topPosition += panel.Height + offset;
            }
        }

        private void TableVisualizerPanel_MouseDown(object sender, MouseEventArgs e)
        {
            isDragging = true;
            lastMousePosition = e.Location;
        }

        private void TableVisualizerPanel_MouseMove(object sender, MouseEventArgs e)
        {
            if (isDragging)
            {
                int deltaX = e.X - lastMousePosition.X;
                int deltaY = e.Y - lastMousePosition.Y;

                foreach (Control control in TableVisualizerPanel.Controls)
                {
                    control.Location = new Point(control.Location.X + deltaX, control.Location.Y + deltaY);
                }
            }
        }

        private void TableVisualizerPanel_MouseUp(object sender, MouseEventArgs e)
        {
            isDragging = false;
        }
    }
}