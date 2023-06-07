using Frontend.Client.Responses.Parts;
using Frontend.Services;

namespace Frontend.Components
{
    public partial class ExplorerControl : UserControl
    {
        private bool databasesFetched = false;
        private List<DatabaseModel> Databases = new();
        private readonly EditorControl _editorControl;

        public ExplorerControl(EditorControl editorControl)
        {
            InitializeComponent();

            _editorControl = editorControl;
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

        private static TreeNode CreateColumnNodes(DatabaseModel.Table table)
        {
            TreeNode rootColumns = new("Columns")
            {
                ImageIndex = 8,
                SelectedImageIndex = 8
            };

            foreach (var column in table.Columns)
            {
                TreeNode columnNode = new(column.Name)
                {
                    ImageIndex = 8,
                    SelectedImageIndex = 8
                };

                columnNode.Nodes.Add(new TreeNode($"Type: {column.Type}")
                {
                    ImageIndex = 8,
                    SelectedImageIndex = 8,
                });

                if (column.Length > 0)
                {
                    columnNode.Nodes.Add(new TreeNode($"Length: {column.Length}")
                    {
                        ImageIndex = 8,
                        SelectedImageIndex = 8,
                    });
                }

                rootColumns.Nodes.Add(columnNode);
            }

            return rootColumns;
        }

        private static TreeNode CreateIndexFileNodes(DatabaseModel.Table table)
        {
            TreeNode rootIndexFilesNode = new("Index Files")
            {
                ImageIndex = 6,
                SelectedImageIndex = 6
            };

            foreach (var indexFile in table.IndexFiles)
            {
                TreeNode indexFileNode = new(indexFile.IndexFileName)
                {
                    ImageIndex = 6,
                    SelectedImageIndex = 6
                };

                rootIndexFilesNode.Nodes.Add(indexFileNode);
            }

            return rootIndexFilesNode;
        }

        private static TreeNode CreatePrimaryKeyNodes(DatabaseModel.Table table)
        {
            TreeNode rootPrimaryKeyNode = new("Primary keys")
            {
                ImageIndex = 3,
                SelectedImageIndex = 3
            };

            foreach (var primaryKey in table.PrimaryKeys)
            {
                TreeNode primaryKeyNode = new(primaryKey)
                {
                    ImageIndex = 3,
                    SelectedImageIndex = 3
                };

                rootPrimaryKeyNode.Nodes.Add(primaryKeyNode);
            }

            return rootPrimaryKeyNode;
        }

        private static TreeNode CreateForeignKeyNodes(DatabaseModel.Table table)
        {
            TreeNode rootForeignKeyNode = new("Foreign Keys")
            {
                ImageIndex = 4,
                SelectedImageIndex = 4
            };

            foreach (var foreignKey in table.ForeignKeys)
            {
                TreeNode foreignKeyNode = new(foreignKey.AttributeName)
                {
                    ImageIndex = 4,
                    SelectedImageIndex = 4
                };

                rootForeignKeyNode.Nodes.Add(foreignKeyNode);
            }

            return rootForeignKeyNode;
        }

        private static TreeNode CreateUniqueKeyNodes(DatabaseModel.Table table)
        {
            TreeNode rootUniqueKeyNode = new("Unique Keys")
            {
                ImageIndex = 5,
                SelectedImageIndex = 5
            };

            foreach (var uniqueKey in table.UniqueKeys)
            {
                TreeNode uniqueKeyNode = new(uniqueKey)
                {
                    ImageIndex = 5,
                    SelectedImageIndex = 5
                };

                rootUniqueKeyNode.Nodes.Add(uniqueKeyNode);
            }

            return rootUniqueKeyNode;
        }

        private void AddFolderToEditorTree(TreeNode root, string folderPath)
        {
            root.Text = Path.GetFileName(folderPath);
            root.Name = folderPath;
            root.Nodes.Clear();

            foreach (string folder in Directory.GetDirectories(folderPath))
            {
                TreeNode newNode = new()
                {
                    ImageIndex = 0,
                    SelectedImageIndex = 0
                };
                root.Nodes.Add(newNode);
                AddFolderToEditorTree(newNode, folder);
            }

            foreach (string file in Directory.GetFiles(folderPath))
            {
                root.Nodes.Add(new TreeNode()
                {
                    ImageIndex = 1,
                    SelectedImageIndex = 1,
                    Text = Path.GetFileName(file),
                    Name = file
                });
            }
        }

        public void OpenFolder(string folderPath)
        {
            ExplorerTree.BeginUpdate();

            AddFolderToEditorTree(ExplorerTree.Nodes[0], folderPath);

            ExplorerTree.Nodes[0].Expand();
            ExplorerTree.EndUpdate();
        }

        private async void CreateDatabasesTree()
        {
            if (databasesFetched)
            {
                return;
            }

            await FetchDatabases();
            databasesFetched = true;

            ExplorerTree.BeginUpdate();
            ExplorerTree.Nodes[1].Nodes.Clear();

            foreach (var database in Databases)
            {
                if (database == null) continue;

                TreeNode databaseNode = new(database.DatabaseName)
                {
                    ImageIndex = 2,
                    SelectedImageIndex = 2
                };

                foreach (var table in database.Tables)
                {
                    TreeNode tableNode = new(table.Name)
                    {
                        ImageIndex = 7,
                        SelectedImageIndex = 7
                    };

                    tableNode.Nodes.Add(CreateColumnNodes(table));
                    tableNode.Nodes.Add(CreatePrimaryKeyNodes(table));
                    tableNode.Nodes.Add(CreateForeignKeyNodes(table));
                    tableNode.Nodes.Add(CreateUniqueKeyNodes(table));
                    tableNode.Nodes.Add(CreateIndexFileNodes(table));

                    databaseNode.Nodes.Add(tableNode);
                }

                ExplorerTree.Nodes[1].Nodes.Add(databaseNode);
            }

            ExplorerTree.Nodes[1].Expand();
            ExplorerTree.EndUpdate();
        }

        private void ExplorerTree_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (e.Node.Name == "databases")
            {
                CreateDatabasesTree();
                return;
            }

            if (!File.Exists(e.Node.Name) || Path.GetExtension(e.Node.Text) != ".sql")
            {
                return;
            }

            foreach (TabPage tab in _editorControl.TabControl.TabPages)
            {
                if (tab.Name == e.Node.Name)
                {
                    _editorControl.TabControl.SelectedTab = tab;
                    return;
                }
            }

            _editorControl.CreateTextEditorTabWithContent(e.Node.Name);
        }
    }
}
