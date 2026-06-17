using System.Net;
using System.Text;
using DataVo.Core.Logging;
using Server.Server.Http;
using Server.Server.Responses;

namespace Server.Server;

internal class HttpServer
{
    private readonly HttpListener _httpListener;
    private readonly string _corsOrigin;

    public HttpServer()
    {
        _httpListener = new HttpListener();
        //_httpListener.Prefixes.Add("http://+:8001/");
        _httpListener.Prefixes.Add("http://localhost:8001/");
        _corsOrigin = GetCorsOrigin();
    }

    public async Task Start()
    {
        Logger.Info("Starting server on port 8001");
        _httpListener.Start();
        Logger.Info("Server listening on port 8001");

        while (true)
        {
            var context = await _httpListener.GetContextAsync();

            _ = Task.Run(() => ProcessRequest(context));
        }
    }

    private async Task ProcessRequest(HttpListenerContext context)
    {
        try
        {
            Logger.Info($"New Request from {context.Request.UserHostName}");

            // Handle CORS headers
            context.Response.Headers.Add("Access-Control-Allow-Origin", _corsOrigin);
            context.Response.Headers.Add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            context.Response.Headers.Add("Access-Control-Allow-Headers", "Content-Type, Accept");

            if (context.Request.HttpMethod == "OPTIONS")
            {
                context.Response.StatusCode = (int)HttpStatusCode.OK;
                context.Response.Close();
                return;
            }

            var response = Router.HandleRequest(context);

            await WriteResponse(context, response);
        }
        catch (Exception ex)
        {
            Logger.Error(ex.ToString());
            await WriteResponse(context, new ErrorResponse(ex));
        }
    }

    public static async Task WriteResponse(HttpListenerContext context, Response response)
    {
        context.Response.ContentType = "application/json";
        context.Response.ContentEncoding = Encoding.UTF8;

        var json = response.ToJson();
        await using var sw = new StreamWriter(context.Response.OutputStream, Encoding.UTF8);
        await sw.WriteAsync(json);
        await sw.FlushAsync();

        context.Response.Close();
    }

    private static string GetCorsOrigin()
    {
        string? configured = Environment.GetEnvironmentVariable("DATAVO_SERVER_CORS_ORIGIN");
        return string.IsNullOrWhiteSpace(configured)
            ? "http://localhost:5173"
            : configured;
    }
}
