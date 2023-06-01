using System.IO;
using System.Windows.Forms;

namespace Frontend.Components
{
    public partial class ExplorerControl : UserControl
    {
        private readonly EditorControl _editorControl;

        public ExplorerControl(EditorControl editorControl)
        {
            InitializeComponent();

            _editorControl = editorControl;
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

        private async void ExplorerTree_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (e.Node.Name == "databases")
            {
                await _editorControl.CreateVisualQueryEditorTab("University");
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
