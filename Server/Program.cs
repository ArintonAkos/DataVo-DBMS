using Server.Logging;
using Server.Server;

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