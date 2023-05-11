using System;
using System.IO;
using System.Windows.Forms;
using Frontend.Client.Responses.Controllers.Parser;
using Frontend.Components;
using Frontend.Services;
using Microsoft.WindowsAPICodePack.Dialogs;

namespace Frontend
{
    public partial class Window : Form
    {
        private ExplorerControl _explorerTree;
        private EditorControl _editorControl;
        private ResponseControl _responseControl;

        public Window()
        {
            InitializeComponent();
        }

        private void Window_Load(object sender, EventArgs e)
        {
            _editorControl = new EditorControl() 
            {
                Dock = DockStyle.Fill,
            };

            _explorerTree = new ExplorerControl(_editorControl)
            {
                Dock = DockStyle.Fill,
            };

            _responseControl = new ResponseControl()
            {
                Dock = DockStyle.Fill,
            };

            SidebarPanel.Controls.Add(_explorerTree);
            EditorPanel.Controls.Add(_editorControl);
            ResponsePanel.Controls.Add(_responseControl);
        }

        private void MenuNewFileOption_Click(object sender, EventArgs e)
        {
            SaveFileDialog saveFileDialog = new SaveFileDialog();
            saveFileDialog.Filter = "SQL files (*.sql)|*.sql";
            saveFileDialog.RestoreDirectory = true;

            if (saveFileDialog.ShowDialog() == DialogResult.OK)
            {
                File.Create(saveFileDialog.FileName);
                _editorControl.CreateTextEditorTab(saveFileDialog.FileName);
            }
        }

        private void MenuOpenFileOption_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            openFileDialog.Filter = "SQL files (*.sql)|*.sql";
            openFileDialog.RestoreDirectory = true;

            if (openFileDialog.ShowDialog() == DialogResult.OK)
            {
                _editorControl.CreateTextEditorTabWithContent(openFileDialog.FileName);
            }
        }

        private void MenuSaveFileOption_Click(object sender, EventArgs e)
        {
            _editorControl.SaveActiveTabContent();
        }

        private void MenuOpenFolderOption_Click(object sender, EventArgs e)
        {
            CommonOpenFileDialog dialog = new CommonOpenFileDialog();
            dialog.RestoreDirectory = true;
            dialog.IsFolderPicker = true;

            if (dialog.ShowDialog() == CommonFileDialogResult.Ok)
            {
                _explorerTree.OpenFolder(dialog.FileName);
            }
        }

        private void MenuExitOption_Click(object sender, EventArgs e)
        {
            Application.Exit();
        }

        private async void StripExecuteButton_Click(object sender, EventArgs e)
        {
            if (_editorControl.TabControl.SelectedIndex < 0)
            {
                return;
            }

            StripExecuteButton.Enabled = false;

            string query = _editorControl.GetActiveTabContent();
            ParseResponse response = await ParseService.GetParseResponse(query);

            _responseControl.HandleResponse(response);

            StripExecuteButton.Enabled = true;
        }
    }
}
