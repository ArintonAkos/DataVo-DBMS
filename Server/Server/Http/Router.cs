using System.Net;
using System.Reflection;
using System.Runtime.ExceptionServices;
using Newtonsoft.Json;
using Server.Server.Http.Attributes;
using Server.Server.Requests;
using Server.Server.Responses;

namespace Server.Server.Http;

internal class Router
{
    private static readonly string _controllerNameSpace = "Server.Server.Http.Controllers";
    private const int DefaultMaxRequestBodyBytes = 1024 * 1024;

    private static List<Type> HttpControllers
    {
        get =>
            Assembly.GetExecutingAssembly()
                .GetTypes()
                .Where(type => type.Namespace == _controllerNameSpace)
                .ToList();
    }

    public static Response HandleRequest(HttpListenerContext request)
    {
        if (request.Request.Url == null)
        {
            throw new ArgumentException("Request URL is required.");
        }

        if (request.Request.Url.Segments.Length < 3)
        {
            throw new ArgumentException("Request path must include controller and action segments.");
        }

        string controllerName = request.Request.Url.Segments[1].Replace("/", "");
        string methodName = request.Request.Url.Segments[2].Replace("/", "");

        var controller = GetController(controllerName);
        var method = GetMethod(controller, methodName);

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
                var requestObject = GetRequest(request.Request, method, GetMaxRequestBodyBytes());

                if (requestObject != null)
                {
                    parameters = new object[] { requestObject, };
                }

                break;
            default:
                throw new NotSupportedException($"Unsupported HTTP method '{httpMethod}'.");
        }

        try
        {
            object? returnValue = method.Invoke(obj: null, parameters);
            return (Response)returnValue!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }

    private static string ValidateHttpMethod(HttpListenerRequest request, MethodInfo method)
    {
        var httpMethod = method.GetCustomAttribute<Method>(inherit: true);

        if (httpMethod == null)
        {
            throw new InvalidOperationException($"Method '{method.Name}' is missing the required HTTP method attribute.");
        }

        if (request.HttpMethod != httpMethod.HttpMethod)
        {
            throw new NotSupportedException($"HTTP method '{request.HttpMethod}' is not supported for '{method.Name}'. Expected '{httpMethod.HttpMethod}'.");
        }

        return httpMethod.HttpMethod;
    }

    private static Type GetController(string controllerName)
    {
        var controller = HttpControllers
            .FirstOrDefault(c =>
                c.GetCustomAttributes<Route>(inherit: true)
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
        var method = controller.GetMethods()
            .FirstOrDefault(m =>
                m.GetCustomAttributes<Route>(inherit: true)
                    ?.Any(p => p.Path == methodName)
                ?? false
            );

        if (method == null)
        {
            throw new MissingMethodException(controller.FullName, methodName);
        }

        return method;
    }

    private static string GetRequestContent(HttpListenerRequest request, int maxRequestBodyBytes)
    {
        if (request.ContentLength64 > maxRequestBodyBytes)
        {
            throw new InvalidOperationException($"Request body exceeds the {maxRequestBodyBytes} byte limit.");
        }

        byte[] buffer = new byte[8192];
        using var memory = new MemoryStream();
        while (true)
        {
            int read = request.InputStream.Read(buffer, 0, buffer.Length);
            if (read == 0)
            {
                break;
            }

            if (memory.Length + read > maxRequestBodyBytes)
            {
                throw new InvalidOperationException($"Request body exceeds the {maxRequestBodyBytes} byte limit.");
            }

            memory.Write(buffer, 0, read);
        }

        return request.ContentEncoding.GetString(memory.ToArray());
    }

    private static Request? GetRequest(HttpListenerRequest request, MethodInfo method, int maxRequestBodyBytes)
    {
        string content = GetRequestContent(request, maxRequestBodyBytes);
        var paramType = method.GetParameters()
            .Select(p => p.ParameterType)
            .FirstOrDefault();

        if (paramType == null)
        {
            return null;
        }

        if (!typeof(Request).IsAssignableFrom(paramType))
        {
            throw new InvalidOperationException($"Request binding type '{paramType.FullName}' is not a Request subtype.");
        }

        var deserializeGeneric = typeof(Router).GetMethod(nameof(DeserializeObject))!;
        var generic = deserializeGeneric.MakeGenericMethod(paramType);

        try
        {
            return (Request?)generic.Invoke(obj: null, new object[] { content, });
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }

    public static T? DeserializeObject<T>(string content) => JsonConvert.DeserializeObject<T>(content);

    private static int GetMaxRequestBodyBytes()
    {
        string? configured = Environment.GetEnvironmentVariable("DATAVO_SERVER_MAX_BODY_BYTES");
        if (string.IsNullOrWhiteSpace(configured))
        {
            return DefaultMaxRequestBodyBytes;
        }

        if (!int.TryParse(configured, out int maxBytes) || maxBytes <= 0)
        {
            throw new InvalidOperationException("DATAVO_SERVER_MAX_BODY_BYTES must be a positive integer.");
        }

        return maxBytes;
    }
}
