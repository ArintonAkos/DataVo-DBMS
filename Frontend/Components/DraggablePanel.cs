using Frontend.Client.Responses.Parts;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Frontend.Components
{
    public partial class DraggablePanel : UserControl
    {
        private Point mouseOffset;
        private DatabaseModel.Table Table { get; set; }

        public DraggablePanel(DatabaseModel.Table table)
        {
            Table = table;
            InitializeComponent();

            foreach (var column in table.Columns)
            {
                listBox1.Items.Add(column.Name);
            }
        }

        private void DraggablePanel_MouseDown(object sender, MouseEventArgs e)
        {
            if (e.Button == MouseButtons.Left)
            {
                mouseOffset = new Point(-e.X, -e.Y);
            }
        }

        private void DraggablePanel_MouseMove(object sender, MouseEventArgs e)
        {
            if (e.Button == MouseButtons.Left)
            {
                var mousePos = MousePosition;
                mousePos.Offset(mouseOffset.X, mouseOffset.Y);
                ((Control)sender).Location = ((Control)sender).Parent.PointToClient(mousePos);
            }
        }
    }
}
