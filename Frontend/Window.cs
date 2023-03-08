using Frontend.Client.Requests;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Windows.Forms;

namespace Frontend
{
    public partial class Window : Form
    {
        static readonly HttpClient httpClient = new HttpClient();

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
            try
            {
                Request request = new Request { CommandType = "", Data = textEditor.Text };

                string jsonObject = JsonConvert.SerializeObject(request);
                StringContent data = new StringContent(jsonObject, Encoding.UTF8, "application/json");

                string url = "http://localhost:8001";
                var response = await httpClient.PostAsync(url, data);
                var result = await response.Content.ReadAsStringAsync();

                tabMessagesText.Text = result;

            } catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
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
