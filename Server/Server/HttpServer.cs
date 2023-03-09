using Server.Parser;
using Server.Server.Responses;
using Server.Logging;
using System.Net;
using Newtonsoft.Json;
using Server.Server.Requests;

namespace Server.Server
{
    internal class HttpServer
    {
        private readonly HttpListener _httpListener;

        public HttpServer()
        {
            _httpListener = new HttpListener();
            _httpListener.Prefixes.Add("http://+:8001/");
        }

        public async Task Start()
        {
            Logger.Info("Starting server on port 8001");
            _httpListener.Start();
            Logger.Info("Server listening on port 8001");

            while (true)
            {
                var context = await _httpListener.GetContextAsync();

                try
                {
                    Response response = ProcessRequest(context);
                    
                    await WriteResponse(context, response);
                } catch (Exception ex)
                {
                    await WriteResponse(context, new Response()
                    {
                        Data = ex.Message,
                        Code = HttpStatusCode.InternalServerError,
                        Meta = "error"
                    });
                }

                
            }
        }

        public Response ProcessRequest(HttpListenerContext context)
        {
            var content = GetRequestContent(context.Request);
            Logger.Info($"New Request from {context.Request.UserHostName}");
            Logger.Info($"{context.Request.UserHostName}: {content}");

            var request = JsonConvert.DeserializeObject<Request>(content);

            if (request == null)
            {
                throw new Exception("Error while parsing data!");
            }

            var parser = new Parser.Parser(request);

            return parser.Parse();
        }

        public async Task WriteResponse(HttpListenerContext context, Response response)
        {
            context.Response.StatusCode = (int)response.Code;

            using (var sw = new StreamWriter(context.Response.OutputStream))
            {
                await sw.FlushAsync();
                sw.Write(response.Data);

                Logger.Info($"Sent Response to {context.Request.UserHostName}: {response.Data}");
            }
        }

        private String GetRequestContent(HttpListenerRequest request)
        {
            string text;

            using (var reader = new StreamReader(request.InputStream, request.ContentEncoding))
            {
                text = reader.ReadToEnd();
            }

            return text;
        }
    }
}
