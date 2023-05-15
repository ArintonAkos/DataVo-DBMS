using Frontend.Client.Responses.Controllers.Parser;
using Frontend.Components;
using Frontend.Services;

namespace Frontend
{
    public partial class Window : Form
    {
        public delegate void DataFetchedHandler(object source, EventArgs args);

        public event DataFetchedHandler DataFetched;

        private ExplorerControl _explorerTree;
        private EditorControl _editorControl;
        private ResponseControl _responseControl;

        public Window()
        {
            InitializeComponent();

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
        }

        private void Window_Load(object sender, EventArgs e)
        {
            SidebarPanel.Controls.Add(_explorerTree);
            EditorPanel.Controls.Add(_editorControl);
            ResponsePanel.Controls.Add(_responseControl);
        }

        private async void MenuNewFileOption_Click(object sender, EventArgs e)
        {
            SaveFileDialog saveFileDialog = new SaveFileDialog();
            saveFileDialog.Filter = "SQL files (*.sql)|*.sql";
            saveFileDialog.RestoreDirectory = true;

            if (saveFileDialog.ShowDialog() == DialogResult.OK)
            {
                File.Create(saveFileDialog.FileName);
                await _editorControl.CreateTextEditorTab(saveFileDialog.FileName);
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
            using FolderBrowserDialog dialog = new();

            if (dialog.ShowDialog() == DialogResult.OK)
            {
                _explorerTree.OpenFolder(dialog.SelectedPath);
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
            Guid session = _editorControl.GetActiveTabSession();
            ParseResponse response = await ParseService.GetParseResponse(query, session);

            _responseControl.HandleResponse(response);

            StripExecuteButton.Enabled = true;
        }
    }
}
