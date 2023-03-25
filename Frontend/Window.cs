using System;
using System.IO;
using System.Windows.Forms;
using Microsoft.WindowsAPICodePack.Dialogs;

namespace Frontend
{
    public partial class Window : Form
    {
        public Window()
        {
            InitializeComponent();
        }

        private void InitializeEditorTree()
        {
            EditorTree.BeginUpdate();
            EditorTree.Nodes.Add(new TreeNode("Visual Query Designers"));
            EditorTree.Nodes.Add(new TreeNode("Open folder"));
            EditorTree.EndUpdate();
        }

        private void OpenFolderForEditorTree(string directory)
        {
            EditorTree.BeginUpdate();
            EditorTree.Nodes[0].Text = Path.GetFileName(directory);

            foreach (string file in Directory.GetFiles(directory))
            {
                EditorTree.Nodes[0].Nodes.Add(new TreeNode(Path.GetFileName(file)));
            }

            EditorTree.Nodes[0].Expand();
            EditorTree.EndUpdate();
        }

        private void CreateNewEditorTab(string filename)
        {
            TabPage tabPage = new TabPage(filename);
            EditorTabControl.TabPages.Add(tabPage);

            RichTextBox textBox = new RichTextBox();
            textBox.Dock = DockStyle.Fill;
            textBox.BorderStyle = BorderStyle.None;

            tabPage.Controls.Add(textBox);
        }

        private RichTextBox GetEditorTextBox(int index)
        {
            return (RichTextBox)EditorTabControl.TabPages[index].Controls[0];
        }

        private void Window_Load(object sender, EventArgs e)
        {
            InitializeEditorTree();

            EditorTabControl.TabPages.Clear();
        }

        private void MenuNewFileOption_Click(object sender, EventArgs e)
        {
            SaveFileDialog saveFileDialog = new SaveFileDialog();
            saveFileDialog.Filter = "SQL files (*.sql)|*.sql";
            saveFileDialog.RestoreDirectory = true;

            if (saveFileDialog.ShowDialog() == DialogResult.OK)
            {
                File.Create(saveFileDialog.FileName);
                CreateNewEditorTab(Path.GetFileName(saveFileDialog.FileName));
            }
        }

        private void MenuOpenFileOption_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            openFileDialog.Filter = "SQL files (*.sql)|*.sql";
            openFileDialog.RestoreDirectory = true;

            if (openFileDialog.ShowDialog() == DialogResult.OK)
            {
                CreateNewEditorTab(Path.GetFileName(openFileDialog.FileName));
                
                RichTextBox textBox = GetEditorTextBox(EditorTabControl.TabPages.Count - 1);
                textBox.Text = File.ReadAllText(openFileDialog.FileName);
            }
        }

        private void MenuOpenFolderOption_Click(object sender, EventArgs e)
        {
            CommonOpenFileDialog dialog = new CommonOpenFileDialog();
            dialog.RestoreDirectory = true;
            dialog.IsFolderPicker = true;

            if (dialog.ShowDialog() == CommonFileDialogResult.Ok)
            {
                OpenFolderForEditorTree(dialog.FileName);
            }
        }

        private void MenuExitOption_Click(object sender, EventArgs e)
        {
            Application.Exit();
        }

        private void StripExecuteButton_Click(object sender, EventArgs e)
        {

        }
    }
}
