using Newtonsoft.Json;
using Server.Server.Http.Attributes;
using Server.Server.Requests;
using Server.Server.Responses;
using System.Data;
using System.Net;
using System.Reflection;

namespace Server.Server.Http
{
    internal class Router
    {
        private static readonly string _controllerNameSpace = "Server.Server.Http.Controllers";

        public static Response HandleRequest(HttpListenerContext request)
        {
            if (request.Request.Url == null)
            {
                throw new Exception("Error parsing request!");
            }

            if (request.Request.Url.Segments.Length < 2)
            {
                throw new Exception("Invalid URL path!");
            }

            string controllerName = request.Request.Url.Segments[1].Replace("/", "");
            string methodName = request.Request.Url.Segments[2].Replace("/", "");

            Type controller = GetController(controllerName);
            MethodInfo method = GetMethod(controller, methodName);

            string httpMethod = ValidateHttpMethod(request.Request, method);

            switch (httpMethod)
            {
                case "GET":
                    var dict = request.Request.QueryString;

                    object[] @params = method.GetParameters()
                            .Select((p, i) => Convert.ChangeType(dict[p.Name], p.ParameterType))
                            .ToArray()!;

                    return (Response)method.Invoke(null, @params)!;
                case "POST":
                    var requestObject = GetRequest(request.Request, method);
                    
                    if (requestObject == null)
                    {
                        return (Response)method.Invoke(null, null)!;
                    }

                    return (Response)method.Invoke(null, new object[] { requestObject })!;
                default:
                    throw new Exception("Unsupported HTTP method!");
            }
        }

        private static string ValidateHttpMethod(HttpListenerRequest request, MethodInfo method)
        {
            Method? httpMethod = method.GetCustomAttributes(true)
                .OfType<Method>()
                .FirstOrDefault();

            if (httpMethod == null)
            {
                throw new Exception("Method attribute not found!");
            }

            if (request.HttpMethod != httpMethod.HttpMethod)
            {
                throw new Exception("HTTP method not found!");
            }

            return httpMethod.HttpMethod;
        }

        private static List<Type> HttpControllers
        {
            get
            {
                return Assembly.GetExecutingAssembly()
                    .GetTypes()
                    .Where(type => type.Namespace == _controllerNameSpace)
                    .ToList();
            }
        }

        private static Type GetController(string controllerName)
        {
            Type? controller = HttpControllers
                .FirstOrDefault(c => ValidateControllerAlias(c, controllerName));

            if (controller == null)
            {
                throw new Exception("Controller not found!");
            }

            return controller;
        }

        private static MethodInfo GetMethod(Type controller, string methodName)
        {
            MethodInfo? method = controller.GetMethods()
                .FirstOrDefault(m => ValidateMethodAlias(m, methodName));

            if (method == null)
            {
                throw new Exception("Method not found!");
            }

            return method;
        }

        private static bool ValidateControllerAlias(Type controller, string path)
        {
            return controller
                .GetCustomAttributes<Route>(true)
                .Any(route => route.Path == path);
        }

        private static bool ValidateMethodAlias(MethodInfo method, string path)
        {
            return method
                .GetCustomAttributes(true)
                .OfType<Route>()
                .Any(route => route.Path == path);
        }

        private static String GetRequestContent(HttpListenerRequest request)
        {
            string text;

            using (var reader = new StreamReader(request.InputStream, request.ContentEncoding))
            {
                text = reader.ReadToEnd();
            }

            return text;
        }

        private static Request? GetRequest(HttpListenerRequest request, MethodInfo method)
        {
            var content = GetRequestContent(request);
            var paramType = method.GetParameters()
                        .Select(p => p.ParameterType)
                        .FirstOrDefault();

            if (paramType == null)
            {
                return null;
            }

            MethodInfo deserializeGeneric = typeof(Router).GetMethod(nameof(Router.DeserializeObject))!;
            MethodInfo generic = deserializeGeneric.MakeGenericMethod(paramType);

            return (Request?)generic.Invoke(null, new object[] { content });
        }

        public static T? DeserializeObject<T>(string content)
        {
            return JsonConvert.DeserializeObject<T>(content);
        }

    }
}
