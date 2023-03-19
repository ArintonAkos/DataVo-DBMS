using Server.Server.Responses;

namespace Server.Parser.Actions
{
    internal interface IDbAction
    {
        public ActionResponse Perform(string session);
    }
}
