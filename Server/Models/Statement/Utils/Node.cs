using Server.Enums;
using Server.Models.Catalog;
using Server.Utils;

namespace Server.Models.Statement.Utils;

/// <summary>
/// Represents a single node in a tree structure. 
/// Each node can represent a value, column, operator 
/// or one of the logical constructs (And, Or, Eq). 
/// Each node has a left and a right child node.
/// </summary>
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

    /// <summary>
    /// The left child of the current node.
    /// </summary>
    public Node? Left { get; set; }
    
    /// <summary>
    /// The right child of the current node.
    /// </summary>
    public Node? Right { get; set; }

    /// <summary>
    /// The type of the node. Can be any of the enum values in NodeType.
    /// </summary>
    public NodeType Type { get; init; }

    /// <summary>
    /// The value held by the node. It can be an operator, string, int, double, boolean, date or null value.
    /// </summary>
    public NodeValue Value { get; init; }

    /// <summary>
    /// Handles the algebraic expression using the operator and the other Node.
    /// </summary>
    /// <param name="operator">The operator used in the algebraic expression.</param>
    /// <param name="other">The other Node used in the algebraic expression.</param>
    /// <returns>A new Node that represents the result of the algebraic expression.</returns>
    public Node HandleAlgebraicExpression(string @operator, Node other) => new()
    { Type = NodeType.Value, Value = Value.SolveAlgebraicExpression(@operator, other.Value), };

    public Node Clone()
    {
        return new Node
        {
            Left = Left?.Clone(),
            Right = Right?.Clone(),
            Type = Type,
            Value = new NodeValue(Value.Value ?? string.Empty) { ValueType = Value.ValueType }
        };
    }

    /// <summary>
    /// Represents the value held by a Node instance. 
    /// Each NodeValue can hold a string, int, double, boolean, date,
    /// null or operator value. 
    /// It also contains a ValueType property that describes the type of the value.
    /// </summary>
    public class NodeValue
    {
        /// <summary>
        /// The value held by the NodeValue. It can be an operator, string, int, double, boolean, date or null value.
        /// </summary>
        public IComparable? Value;

        /// <summary>
        /// The type of the value held by the NodeValue.
        /// </summary>
        public NodeValueType ValueType;

        /// <summary>
        /// Returns the Value property converted to its appropriate type.
        /// </summary>
        public dynamic? ParsedValue
        {
            get
            {
                return ConvertGenericToType(Value, ValueType.ToType());
            }
        }

        /// <summary>
        /// Initializes a new instance of the NodeValue class with a specified value and value type.
        /// </summary>
        /// <param name="value">The IComparable value to initialize the NodeValue instance with.</param>
        /// <param name="valueType">The NodeValueType to initialize the NodeValue instance with.</param>
        private NodeValue(IComparable value, NodeValueType valueType)
        {
            Value = value;
            ValueType = valueType;
        }

        /// <summary>
        /// Initializes a new instance of the NodeValue class with a specified value.
        /// </summary>
        /// <param name="value">The value to initialize the NodeValue instance with.</param>
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
        /// Attempts to parse a raw value to a known primitive type and returns a new NodeValue 
        /// instance containing the parsed value.
        /// </summary>
        /// <param name="rawValue">The raw value to be parsed.</param>
        /// <returns>The parsed NodeValue instance.</returns>
        /// <exception cref="Exception">
        /// Thrown when the given parameter cannot be
        /// parsed as any known primitive type.
        /// </exception>
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
        /// Attempts to parse a raw value as a logical operator and
        /// returns a new NodeValue instance containing the operator.
        /// </summary>
        /// <param name="rawValue">The raw value to be parsed as a logical operator.</param>
        /// <returns>The parsed NodeValue instance.</returns>
        /// <exception cref="Exception">
        /// Thrown when the given parameter 
        /// is not a known logical operator.
        /// </exception>
        public static NodeValue Operator(string rawValue)
        {
            if (!Operators.Supported().Contains(rawValue))
            {
                throw new Exception($"{rawValue} is not a known logical operator!");
            }

            return new NodeValue(rawValue, NodeValueType.Operator);
        }

        /// <summary>
        /// Creates a new NodeValue instance containing a raw string value.
        /// </summary>
        /// <param name="rawValue">The raw string value.</param>
        /// <returns>The new NodeValue instance containing the raw string value.</returns>
        public static NodeValue RawString(string rawValue) => new(rawValue, NodeValueType.String);

        /// <summary>
        /// Solves an algebraic expression using the operator and the other NodeValue instance.
        /// </summary>
        /// <param name="operator">The operator used in the algebraic expression.</param>
        /// <param name="other">The other NodeValue used in the algebraic expression.</param>
        /// <returns>A new NodeValue that represents the result of the algebraic expression.</returns>
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

        /// <summary>
        /// Handles arithmetic operations based on the specified operator and the other NodeValue.
        /// </summary>
        /// <param name="operator">The operator used in the arithmetic operation.</param>
        /// <param name="other">The other NodeValue used in the arithmetic operation.</param>
        /// <returns>The result of the arithmetic operation as a dynamic object.</returns>
        /// <exception cref="Exception">Thrown when an invalid operator is used for the types.</exception>
        private dynamic HandleArithmeticOperators(string @operator, NodeValue other)
        {
            ValidateAlgebraicExpression(@operator, other);

            dynamic? typedValue = ConvertGenericToType(Value, ValueType.ToType());
            dynamic? typedOtherValue = ConvertGenericToType(other.Value, other.ValueType.ToType());

            return @operator switch
            {
                "+" => typedValue + typedOtherValue,
                "-" => typedValue - typedOtherValue,
                "*" => typedValue * typedOtherValue,
                "/" => typedValue / typedOtherValue,
                _ => throw new Exception($"Invalid operator: {@operator} for types!"),
            };
        }

        /// <summary>
        /// Validates the algebraic expression to ensure it can be solved.
        /// </summary>
        /// <param name="operator">The operator used in the algebraic expression.</param>
        /// <param name="other">The other NodeValue used in the algebraic expression.</param>
        /// <exception cref="Exception">Thrown when an arithmetic operator is used on non-numeric types.</exception>
        private void ValidateAlgebraicExpression(string @operator, NodeValue other)
        {
            if (!ValueType.IsNumeric() || !other.ValueType.IsNumeric())
            {
                throw new Exception("Arithmetic operator can only be used for numeric types!");
            }

            if (!Operators.ArithmeticOperators.Contains(@operator))
            {
                throw new Exception("Invalid arithmetic operator: " + @operator);
            }
        }

        /// <summary>
        /// Converts a generic IComparable to a specific type.
        /// </summary>
        /// <param name="comparable">The IComparable to be converted.</param>
        /// <param name="type">The Type to convert the IComparable to.</param>
        /// <returns>The converted value as a dynamic object.</returns>
        private static dynamic? ConvertGenericToType(IComparable? comparable, Type type) =>
            Convert.ChangeType(comparable, type);
    }
}