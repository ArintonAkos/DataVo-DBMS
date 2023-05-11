using System;
using System.Drawing;
using System.Windows.Forms;

namespace Frontend.Components
{
    public partial class VisualQueryControl : UserControl
    {
        private readonly Panel draggablePanel1;
        private readonly Panel draggablePanel2;
        private readonly Button joinButton;
        private readonly Button selectButton;
        private Point mouseOffset;
        private readonly TextBox sqlTextBox;
        private ListBox table1ListBox, table2ListBox;

        public VisualQueryControl()
        {
            InitializeComponent();

            CreateDraggablePanel(ref draggablePanel1, new Point(x: 10, y: 10), "Table1",
                new[] { "Column1_1", "Column1_2", });
            CreateDraggablePanel(ref draggablePanel2, new Point(x: 220, y: 10), "Table2",
                new[] { "Column2_1", "Column2_2", });

            joinButton = new Button
            {
                Text = "Join",
                Location = new Point(x: 10, y: 210),
                Size = new Size(width: 100, height: 25),
            };
            joinButton.Click += JoinButton_Click;

            selectButton = new Button
            {
                Text = "Select",
                Location = new Point(x: 120, y: 210),
                Size = new Size(width: 100, height: 25),
            };
            selectButton.Click += SelectButton_Click;

            sqlTextBox = new TextBox
            {
                Location = new Point(x: 10, y: 240),
                Size = new Size(width: 400, height: 200),
                Multiline = true,
                ReadOnly = true,
            };

            Controls.Add(draggablePanel1);
            Controls.Add(draggablePanel2);
            Controls.Add(joinButton);
            Controls.Add(selectButton);
            Controls.Add(sqlTextBox);
        }

        private void CreateDraggablePanel(ref Panel panel, Point location, string tableName, string[] columnNames)
        {
            panel = new Panel
            {
                Size = new Size(width: 200, height: 200),
                BackColor = Color.LightGray,
                Location = location,
            };
            panel.MouseDown += DraggablePanel_MouseDown;
            panel.MouseMove += DraggablePanel_MouseMove;

            var label = new Label { Text = tableName, Location = new Point(x: 10, y: 10), };
            panel.Controls.Add(label);

            var listBox = new ListBox { Location = new Point(x: 10, y: 30), Size = new Size(width: 180, height: 160), };
            listBox.Items.AddRange(columnNames);
            panel.Controls.Add(listBox);

            if (tableName == "Table1")
            {
                table1ListBox = listBox;
            }
            else if (tableName == "Table2")
            {
                table2ListBox = listBox;
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

        private void JoinButton_Click(object sender, EventArgs e)
        {
            string table1SelectedColumn = table1ListBox.SelectedItem?.ToString();
            string table2SelectedColumn = table2ListBox.SelectedItem?.ToString();

            if (table1SelectedColumn != null && table2SelectedColumn != null)
            {
                sqlTextBox.Text =
                    $"SELECT * FROM Table1 INNER JOIN Table2 ON Table1.{table1SelectedColumn} = Table2.{table2SelectedColumn};";
            }
            else
            {
                sqlTextBox.Text = "You must select one column from each table for a join operation.";
            }
        }

        private void SelectButton_Click(object sender, EventArgs e)
        {
            string table1SelectedColumn = table1ListBox.SelectedItem?.ToString();
            string table2SelectedColumn = table2ListBox.SelectedItem?.ToString();

            if (table1SelectedColumn != null && table2SelectedColumn != null)
            {
                sqlTextBox.Text =
                    $"SELECT Table1.{table1SelectedColumn}, Table2.{table2SelectedColumn} FROM Table1, Table2;";
            }
            else
            {
                sqlTextBox.Text = "You must select one column from each table for a select operation.";
            }
        }
    }
}