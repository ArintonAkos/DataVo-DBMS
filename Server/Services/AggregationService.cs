using Server.Models.Statement.Utils;
using Server.Parser.Aggregations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Services
{
    internal class AggregationService
    {
        private static readonly Dictionary<string, Type> _aggregationFunctions = new(StringComparer.OrdinalIgnoreCase)
        {
            { "avg", typeof(Avg) },
            { "count", typeof(Count) },
            { "max", typeof(Max) },
            { "min", typeof(Min) },
            { "sum", typeof(Sum) },
        };

        public static Aggregation CreateInstance(string functionName, Column column)
        {
            if (!_aggregationFunctions.TryGetValue(functionName, out var type))
            {
                throw new ArgumentException($"Unknown aggregation function: {functionName}");
            }

            return (Aggregation)Activator.CreateInstance(type, column)!;
        }
    }
}
