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

            if (request.Request.Url.Segments.Length < 3)
            {
                throw new Exception("Invalid URL path!");
            }

            string controllerName = request.Request.Url.Segments[1].Replace("/", "");
            string methodName = request.Request.Url.Segments[2].Replace("/", "");

            Type controller = GetController(controllerName);
            MethodInfo method = GetMethod(controller, methodName);

            string httpMethod = ValidateHttpMethod(request.Request, method);
            object[]? parameters = null;

            switch (httpMethod)
            {
                case "GET":
                    var dict = request.Request.QueryString;

                    parameters = method.GetParameters()
                            ?.Select((p, i) => Convert.ChangeType(dict[p.Name], p.ParameterType))
                            ?.ToArray()!;
                    break;
                case "POST":
                    var requestObject = GetRequest(request.Request, method);
                    
                    if (requestObject != null)
                    {
                        parameters = new object[] { requestObject };
                    }
                    
                    break;
                default:
                    throw new Exception("Unsupported HTTP method!");
            }

            try
            {
                var returnValue = method.Invoke(null, parameters);
                return (Response)returnValue!;
            }
            catch (Exception ex)
            {
                while (ex.InnerException != null)
                {
                    ex = ex.InnerException;
                }

                throw ex;
            }
        }

        private static string ValidateHttpMethod(HttpListenerRequest request, MethodInfo method)
        {
            Method? httpMethod = method.GetCustomAttribute<Method>(true);

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

        private static List<Type> HttpControllers => Assembly.GetExecutingAssembly()
            .GetTypes()
            .Where(type => type.Namespace == _controllerNameSpace)
            .ToList();

        private static Type GetController(string controllerName)
        {
            Type? controller = HttpControllers
                .FirstOrDefault(c =>
                    c.GetCustomAttributes<Route>(true)
                        ?.Any(p => p.Path == controllerName) 
                    ?? false
                );

            if (controller == null)
            {
                throw new ArgumentException("Controller not found.");
            }

            return controller;
        }

        private static MethodInfo GetMethod(Type controller, string methodName)
        {
            MethodInfo? method = controller.GetMethods()
                .FirstOrDefault(m => 
                    m.GetCustomAttributes<Route>(true)
                        ?.Any(p => p.Path == methodName)
                    ?? false
                );

            if (method == null)
            {
                throw new Exception("Method not found!");
            }

            return method;
        }

        private static String GetRequestContent(HttpListenerRequest request)
        {
            using var reader = new StreamReader(request.InputStream, request.ContentEncoding);
            return reader.ReadToEnd();
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

            try
            {
                return (Request?)generic.Invoke(null, new object[] { content });
            }
            catch (Exception ex) 
            {
                throw ex.InnerException ?? ex;
            }
        }

        public static T? DeserializeObject<T>(string content)
        {
            return JsonConvert.DeserializeObject<T>(content);
        }

    }
}
