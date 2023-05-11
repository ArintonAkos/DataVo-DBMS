using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Frontend.Components
{
    public partial class DraggablePanel : Component
    {
        public DraggablePanel()
        {
            InitializeComponent();
        }

        public DraggablePanel(IContainer container)
        {
            container.Add(this);

            InitializeComponent();
        }
    }
}
