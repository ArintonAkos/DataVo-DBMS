using System;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Windows.Forms;
using Frontend.Client.Requests;
using Frontend.Client.Responses;
using Frontend.Client.Responses.Controllers.Parser;
using Frontend.Services;
using Microsoft.WindowsAPICodePack.Dialogs;

namespace Frontend
{
    public partial class Window : Form
    {
        public Window()
        {
            InitializeComponent();
        }

        private void AddFolderToEditorTree(TreeNode root, string folderPath)
        {
            root.Text = Path.GetFileName(folderPath);
            root.Name = folderPath;
            root.Nodes.Clear();

            foreach (string folder in Directory.GetDirectories(folderPath))
            {
                root.Nodes.Add(new TreeNode());
                AddFolderToEditorTree(root.Nodes[root.Nodes.Count - 1], folder);
            }

            foreach (string file in Directory.GetFiles(folderPath))
            {
                root.Nodes.Add(new TreeNode()
                {
                    Text = Path.GetFileName(file),
                    Name = file
                });
            }
        }

        private void OpenFolderForEditorTree(string folderPath)
        {
            EditorTree.BeginUpdate();

            AddFolderToEditorTree(EditorTree.Nodes[0], folderPath);

            EditorTree.Nodes[0].Expand();
            EditorTree.EndUpdate();
        }

        private void CreateNewEditorTab(string filePath)
        {
            TabPage tabPage = new TabPage(Path.GetFileName(filePath));
            tabPage.Name = filePath;

            RichTextBox textBox = new RichTextBox();
            textBox.Dock = DockStyle.Fill;
            textBox.BorderStyle = BorderStyle.None;
            textBox.Font = new Font("Consolas", 11);

            tabPage.Controls.Add(textBox);

            EditorTabControl.TabPages.Add(tabPage);
        }

        private void CreateNewEditorTabWithContent(string filePath)
        {
            CreateNewEditorTab(filePath);

            RichTextBox textBox = GetEditorTextBox(EditorTabControl.TabCount - 1);
            textBox.Text = File.ReadAllText(filePath);
        }

        private RichTextBox GetEditorTextBox(int index)
        {
            return (RichTextBox)EditorTabControl.TabPages[index].Controls[0];
        }

        private void Window_Load(object sender, EventArgs e)
        {
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
                CreateNewEditorTab(saveFileDialog.FileName);
            }
        }

        private void MenuOpenFileOption_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            openFileDialog.Filter = "SQL files (*.sql)|*.sql";
            openFileDialog.RestoreDirectory = true;

            if (openFileDialog.ShowDialog() == DialogResult.OK)
            {
                CreateNewEditorTabWithContent(openFileDialog.FileName);
            }
        }

        private void MenuSaveFileOption_Click(object sender, EventArgs e)
        {
            if (EditorTabControl.SelectedIndex >= 0)
            {
                RichTextBox textBox = GetEditorTextBox(EditorTabControl.SelectedIndex);
                File.WriteAllText(EditorTabControl.SelectedTab.Name, textBox.Text);
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

        private async void StripExecuteButton_Click(object sender, EventArgs e)
        {
            if (EditorTabControl.SelectedIndex < 0)
            {
                return;
            }

            ParseResponse response = await HttpService.Post(new Request()
            {
                Data = GetEditorTextBox(EditorTabControl.SelectedIndex).Text
            });

            foreach (ScriptResponse scriptResp in response.Data)
            {
                foreach (ActionResponse actionResp in scriptResp.Actions)
                {
                    actionResp.Messages.ForEach(message => ResponseTabMessagesText.Text += message + "\n");
                }
            }

            ResponseTabPanel.SelectedTab = tabMessages;
        }

        private void EditorTree_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (!File.Exists(e.Node.Name) || Path.GetExtension(e.Node.Text) != ".sql")
            {
                return;
            }

            foreach (TabPage tab in EditorTabControl.TabPages)
            {
                if (tab.Name == e.Node.Name)
                {
                    EditorTabControl.SelectedTab = tab;
                    return;
                }
            }

            CreateNewEditorTabWithContent(e.Node.Name);
        }

        private void EditorTabControl_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Modifiers == Keys.Control && e.KeyCode == Keys.S)
            {
                MenuSaveFileOption_Click(this, e);
            }
        }

        private void EditorTabPage_MouseClicked(object sender, MouseEventArgs e)
        {
            var tabControl = sender as TabControl;
            var tabs = tabControl.TabPages;

            if (e.Button == MouseButtons.Middle)
            {
                tabs.Remove(tabs.Cast<TabPage>()
                        .Where((t, i) => tabControl.GetTabRect(i).Contains(e.Location))
                        .First());
            }
        }
    }
}
