using Frontend.Client.Requests;
using Frontend.Client.Responses;
using Frontend.Services;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Windows.Forms;

namespace Frontend
{
    public partial class Window : Form
    {
        public Window()
        {
            InitializeComponent();
        }

        private void Window_Load(object sender, EventArgs e)
        {
            stripFilenameLabel.Text = string.Empty;
        }

        private async void stripExecute_Click(object sender, EventArgs e)
        {
            Response response = await HttpService.Post(new Request 
            { 
                Data = textEditor.Text 
            });

            tabMessagesText.Text = response.Data;
        }

        private void menuNewFile_Click(object sender, EventArgs e)
        {
            SaveFileDialog fileChooser = new SaveFileDialog();

            fileChooser.InitialDirectory = "c:\\";
            fileChooser.Filter = "SQL files (*.sql)|*.sql";
            fileChooser.RestoreDirectory = true;

            if (fileChooser.ShowDialog() == DialogResult.OK)
            {
                stripFilenameLabel.Text = fileChooser.FileName;
                File.Create(stripFilenameLabel.Text);
            }
        }

        private void menuOpenFile_Click(object sender, EventArgs e)
        {
            OpenFileDialog fileChooser = new OpenFileDialog();

            fileChooser.InitialDirectory = "c:\\";
            fileChooser.Filter = "SQL files (*.sql)|*.sql";
            fileChooser.RestoreDirectory = true;

            if (fileChooser.ShowDialog() == DialogResult.OK)
            {
                stripFilenameLabel.Text = fileChooser.FileName;
                textEditor.Text = File.ReadAllText(stripFilenameLabel.Text);
            }
        }

        private void menuSaveFile_Click(object sender, EventArgs e)
        {
            File.WriteAllText(stripFilenameLabel.Text, textEditor.Text);
        }

        private void menuExit_Click(object sender, EventArgs e)
        {
            Application.Exit();
        }

        private void textEditor_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Modifiers == Keys.Control && e.KeyCode == Keys.S)
            {
                menuSaveFile_Click(this, e);
            }
        }
    }
}
