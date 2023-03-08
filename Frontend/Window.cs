using System;
using System.IO;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Xml.Schema;

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

        private void stripExecute_Click(object sender, EventArgs e)
        {

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
