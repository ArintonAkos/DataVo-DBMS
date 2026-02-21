using Server.Server;
using DataVo.Core.Logging;

try
{
    HttpServer httpServer = new();
    await httpServer.Start();
}
catch (Exception e)
{
    Logger.Error("Stopping SERVER! An error occured!");
    Logger.Error(e.Message);
}