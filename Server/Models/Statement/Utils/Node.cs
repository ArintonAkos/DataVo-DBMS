using Server.Enums;
using Server.Models.Catalog;
using Server.Utils;

namespace Server.Models.Statement.Utils;

public class Node
{
    public enum NodeType
    {
        Value,
        Column,
        Operator,
        And,
        Or,
        Eq,
    }

    public enum NodeValueType
    {
        String,
        Int,
        Double,
        Boolean,
        Date,
        Null, // Any Type can be null, so we need a special type for it
        Operator,
    }

    public Node? Left { get; set; }
    public Node? Right { get; set; }
    public NodeType Type { get; init; }
    public NodeValue Value { get; init; }
    public bool UseIndex { get; set; }

    public Node HandleAlgebraicExpression(string @operator, Node other) => new()
    { Type = NodeType.Value, Value = Value.SolveAlgebraicExpression(@operator, other.Value), };

    public class NodeValue
    {
        public IComparable? Value;
        public NodeValueType ValueType;
        public dynamic? ParsedValue
        {
            get
            {
                return ConvertGenericToType(Value, ValueType.ToType());
            }
        }

        private NodeValue(IComparable value, NodeValueType valueType)
        {
            Value = value;
            ValueType = valueType;
        }

        public NodeValue(dynamic value)
        {
            Value = value;
            ValueType = value.GetType().Name switch
            {
                "String" => NodeValueType.String,
                "Int32" => NodeValueType.Int,
                "Double" => NodeValueType.Double,
                "Boolean" => NodeValueType.Boolean,
                "DateOnly" => NodeValueType.Date,
                _ => NodeValueType.Null,
            };
        }

        /// <summary>
        ///     Factory function to create a new instance of NodeValue by parsing the raw value to known primitive types such as
        ///     string, double, int, bool.
        /// </summary>
        /// <param name="rawValue">The raw value to be parsed</param>
        /// <returns>The parsed NodeValue object</returns>
        /// <exception cref="Exception">Thrown when the given parameter cannot be parsed as any known primitive type.</exception>
        /// <remarks>
        ///     This function first checks if the rawValue is a string enclosed in single quotes (''), then it extracts the string
        ///     value from the quotes.
        ///     If the rawValue can be parsed to an integer or a double, the function returns the corresponding NodeValue object.
        ///     If the rawValue can be parsed to a boolean, the function returns the corresponding NodeValue object.
        ///     If the rawValue is null, the function returns a NodeValue object with a Null value type and a default value of 0.
        /// </remarks>
        public static NodeValue Parse(string? rawValue)
        {
            if (rawValue is null)
            {
                return new NodeValue(value: 0, NodeValueType.Null);
            }

            if (rawValue.StartsWith("'") && rawValue.EndsWith("'"))
            {
                return new NodeValue(rawValue.TruncateLeftRight(charsToTruncate: 1), NodeValueType.String);
            }

            if (int.TryParse(rawValue, out int intValue))
            {
                return new NodeValue(intValue, NodeValueType.Int);
            }

            if (double.TryParse(rawValue, out double doubleValue))
            {
                return new NodeValue(doubleValue, NodeValueType.Double);
            }

            if (bool.TryParse(rawValue, out bool boolValue))
            {
                return new NodeValue(boolValue, NodeValueType.Boolean);
            }

            if (DateOnly.TryParse(rawValue, out DateOnly dateValue))
            {
                return new NodeValue(dateValue, NodeValueType.Date);
            }

            throw new Exception($"{rawValue} is not any known primitive type!");
        }

        /// <summary>
        ///     Factory function to create a new instance of NodeValue representing a logical operator.
        /// </summary>
        /// <param name="rawValue">The raw value to be parsed as a logical operator.</param>
        /// <returns>The parsed NodeValue object.</returns>
        /// <exception cref="Exception">Thrown when the given parameter is not a known logical operator.</exception>
        public static NodeValue Operator(string rawValue)
        {
            if (!Operators.Supported().Contains(rawValue))
            {
                throw new Exception($"{rawValue} is not a known logical operator!");
            }

            return new NodeValue(rawValue, NodeValueType.Operator);
        }

        /// <summary>
        ///     Creates a new instance of NodeValue with the provided raw string value.
        /// </summary>
        /// <param name="rawValue">The raw string value to be stored in the NodeValue.</param>
        /// <returns>A new instance of NodeValue with the specified raw string value.</returns>
        /// <remarks>
        ///     This method converts the provided raw string value to a generic IComparable object using the ConvertValueToGeneric
        ///     helper method.
        ///     The NodeValueType of the returned NodeValue is set to NodeValueType.String, indicating that it stores a string
        ///     value.
        /// </remarks>
        public static NodeValue RawString(string rawValue) => new(rawValue, NodeValueType.String);

        public NodeValue SolveAlgebraicExpression(string @operator, NodeValue other)
        {
            if (ValueType == NodeValueType.Null || other.ValueType == NodeValueType.Null)
            {
                throw new Exception("Cannot solve algebra expression with null values!");
            }

            var currentNodeType = Value!.GetType();
            var otherNodeType = other.Value!.GetType();

            if (currentNodeType != otherNodeType)
            {
                throw new Exception(
                    $"The type of {Value} (Type: {currentNodeType}) is not equal to the type of {other.Value} (Type: {otherNodeType})!");
            }

            ValidateAlgebraicExpression(@operator, other);

            if (!Operators.ArithmeticOperators.Contains(@operator))
            {
                throw new Exception("Invalid arithmetic operator: " + @operator);
            }

            dynamic derivedValue = HandleArithmeticOperators(@operator, other);

            return new NodeValue(derivedValue);
        }

        private dynamic HandleArithmeticOperators(string @operator, NodeValue other)
        {
            ValidateAlgebraicExpression(@operator, other);

            dynamic typedValue = ConvertGenericToType(Value, ValueType.ToType());
            dynamic typedOtherValue = ConvertGenericToType(other.Value, other.ValueType.ToType());

            return @operator switch
            {
                "+" => typedValue + typedOtherValue,
                "-" => typedValue - typedOtherValue,
                "*" => typedValue * typedOtherValue,
                "/" => typedValue / typedOtherValue,
                _ => throw new Exception($"Invalid operator: {@operator} for types!"),
            };
        }

        private void ValidateAlgebraicExpression(string @operator, NodeValue other)
        {
            if (!ValueType.IsNumeric() || !other.ValueType.IsNumeric())
            {
                throw new Exception("Arithmetic operator can only be used for numeric types!");
            }
        }

        private static dynamic? ConvertGenericToType(IComparable? comparable, Type type) =>
            Convert.ChangeType(comparable, type);
    }
}