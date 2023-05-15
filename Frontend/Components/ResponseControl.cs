using Frontend.Client.Responses;
using Frontend.Client.Responses.Controllers.Parser;

namespace Frontend.Components
{
    public partial class ResponseControl : UserControl
    {
        public ResponseControl()
        {
            InitializeComponent();
        }

        internal void HandleResponse(ParseResponse response)
        {
            foreach (ScriptResponse scriptResp in response.Data)
            {
                foreach (ActionResponse actionResp in scriptResp.Actions)
                {
                    actionResp.Messages.ForEach(message => MessagesTextbox.Text += message + "\n");
                }
            }

            tabControl.SelectedTab = MessagesTabPanel;
        }
    }
}
