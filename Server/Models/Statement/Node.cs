using Server.Enums;
using Server.Utils;

namespace Server.Models.Statement
{
    public class Node
    {
        public enum NodeType
        {
            Value,
            Column,
            Operator,
        }

        public enum NodeValueType
        {
            String,
            Int,
            Double,
            Boolean,
            Null, // Any Type can be null, so we need a special type for it
            Operator
        }

        public class NodeValue
        {
            public IComparable? Value;
            public NodeValueType ValueType;

            /// <summary>
            /// Initializes a new instance of the NodeValue class with the specified value and NodeValueType.
            /// </summary>
            /// <param name="value">The value to be held by this NodeValue object.</param>
            /// <param name="valueType">The NodeValueType of the specified value.</param>
            public NodeValue() { }
            public NodeValue(IComparable value, NodeValueType valueType)
            {
                Value = value;
                ValueType = valueType;
            }

            /// <summary>
            /// Factory function to create a new instance of NodeValue by parsing the raw value to known primitive types such as string, double, int, bool.
            /// </summary>
            /// <param name="rawValue">The raw value to be parsed</param>
            /// <returns>The parsed NodeValue object</returns>
            /// <exception cref="Exception">Thrown when the given parameter cannot be parsed as any known primitive type.</exception>
            /// <remarks>
            /// This function first checks if the rawValue is a string enclosed in single quotes (''), then it extracts the string value from the quotes.
            /// If the rawValue can be parsed to an integer or a double, the function returns the corresponding NodeValue object.
            /// If the rawValue can be parsed to a boolean, the function returns the corresponding NodeValue object.
            /// If the rawValue is null, the function returns a NodeValue object with a Null value type and a default value of 0.
            /// </remarks>
            public static NodeValue Parse(string rawValue)
            {
                dynamic parsedValue;
                NodeValueType valueType;

                if (rawValue.StartsWith("'") && rawValue.EndsWith("'"))
                {
                    parsedValue = rawValue.TruncateLeftRight(1);
                    valueType = NodeValueType.String;
                }
                else if (int.TryParse(rawValue, out int intValue))
                {
                    parsedValue = intValue;
                    valueType = NodeValueType.Int;
                }
                else if (double.TryParse(rawValue, out double doubleValue))
                {
                    parsedValue = doubleValue;
                    valueType = NodeValueType.Double;
                }
                else if (bool.TryParse(rawValue, out bool boolValue))
                {
                    parsedValue = boolValue;
                    valueType = NodeValueType.Boolean;
                }
                else if (rawValue == null)
                {
                    parsedValue = 0;
                    valueType = NodeValueType.Null;
                }
                else
                {
                    throw new Exception($"{rawValue} is not any known primitive type!");
                }

                IComparable convertedValue = ConvertValueToGeneric(parsedValue);
                return new(convertedValue, valueType);
            }

            /// <summary>
            /// Factory function to create a new instance of NodeValue representing a logical operator.
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

                IComparable convertedValue = ConvertValueToGeneric(rawValue);
                return new(convertedValue, NodeValueType.Operator);
            }

            /// <summary>
            /// Creates a new instance of NodeValue with the provided raw string value.
            /// </summary>
            /// <param name="rawValue">The raw string value to be stored in the NodeValue.</param>
            /// <returns>A new instance of NodeValue with the specified raw string value.</returns>
            /// <remarks>
            /// This method converts the provided raw string value to a generic IComparable object using the ConvertValueToGeneric helper method.
            /// The NodeValueType of the returned NodeValue is set to NodeValueType.String, indicating that it stores a string value.
            /// </remarks>
            public static NodeValue RawString(string rawValue)
            {
                IComparable convertedValue = ConvertValueToGeneric(rawValue);
                return new(convertedValue, NodeValueType.String);
            }

            /// <summary>
            /// Compares the current NodeValue object with another NodeValue object of the same type.
            /// </summary>
            /// <param name="Operator">A string representing the comparison operator to use, such as ">" or "<=".</param>
            /// <param name="other">The NodeValue object to compare with the current NodeValue object.</param>
            /// <returns>True if the comparison is true, otherwise false.</returns>
            /// <exception cref="Exception">Thrown when the type of this NodeValue object is not equal to the type of the other NodeValue object.</exception>
            public NodeValue Compare(string Operator, NodeValue other)
            {
                if (ValueType == NodeValueType.Null || other.ValueType == NodeValueType.Null)
                {
                    return new NodeValue()
                    {
                        Value = CompareNullValues(Operator, other),
                        ValueType = NodeValueType.Boolean
                    };
                }

                Type currentNodeType = Value!.GetType();
                Type otherNodeType = other.Value!.GetType();

                if (currentNodeType != otherNodeType)
                {
                    throw new Exception($"The type of {Value} (Type: {currentNodeType}) is not equal to the type of {other.Value} (Type: {otherNodeType})!");
                }

                int result = Value.CompareTo(other.Value);

                return new NodeValue() 
                {
                    Value = Operator switch
                    {
                        ">" => result > 0,
                        "<" => result < 0,
                        ">=" => result >= 0,
                        "<=" => result <= 0,
                        "=" => result == 0,
                        "!=" => result != 0,
                        "AND" => (Boolean)Value && (Boolean)other.Value,
                        "OR" => (Boolean)Value || (Boolean)other.Value,
                        "+" or "-" or "*" or "/" => HandleArithmeticOperators(Operator, other),
                        _ => throw new Exception("Invalid operator: " + Operator),
                    },
                    ValueType = NodeValueType.Boolean
                };
            }

            private Boolean HandleArithmeticOperators(string Operator, NodeValue other)
            {
                if (!ValueType.IsNumeric() || !other.ValueType.IsNumeric())
                {
                    throw new Exception($"Arithmetic operator can only be used for numeric types!");
                }

                dynamic typedValue = ConvertGenericToType(Value, ValueType.ToType());
                dynamic typedOtherValue = ConvertGenericToType(other.Value, other.ValueType.ToType());
                
                return Operator switch
                {
                    "+" => typedValue + typedOtherValue,
                    "-" => typedValue - typedOtherValue,
                    "*" => typedValue * typedOtherValue,
                    "/" => typedValue / typedOtherValue,
                    _ => throw new Exception($"Invalid operator: {Operator} for types!"),
                };
            }

            /// <summary>
            /// Compares two NodeValue objects that have a ValueType of Null.
            /// </summary>
            /// <param name="Operator">A string representing the comparison operator to use, such as ">" or "<=".</param>
            /// <param name="other">The NodeValue object to compare with the current NodeValue object.</param>
            /// <returns>True if the comparison is true, otherwise false.</returns>
            private Boolean CompareNullValues(string Operator, NodeValue other)
            {
                return Operator switch
                {
                    ">" or "<" or ">=" or "<=" => false,
                    "=" => other.ValueType == NodeValueType.Null && ValueType == NodeValueType.Null,
                    "!=" => (other.ValueType == NodeValueType.Null) ^ (ValueType == NodeValueType.Null),
                    _ => throw new Exception("Invalid operator: " + Operator),
                };
            }

            /// <summary>
            /// Converts the given dynamic value to the generic type of the NodeValue object.
            /// </summary>
            /// <param name="value">The dynamic value to be converted.</param>
            /// <returns>The converted value.</returns>
            private static IComparable ConvertValueToGeneric(dynamic value)
            {
                return value;                
            }

            private static dynamic ConvertGenericToType(IComparable comparable, Type type)
            {
                return Convert.ChangeType(comparable, type);
            }
        }

        public Node? Left { get; set; } = null;
        public Node? Right { get; set; } = null;
        public NodeType Type { get; set; }
        public NodeValue Value { get; set; }

        public Node FromColumnToNodeValue(IDictionary<string, dynamic> data)
        {
            if (Type != NodeType.Column)
            {
                throw new Exception("Only column nodes can be converted to value!");
            }

            if (Value.ValueType != NodeValueType.String)
            {
                throw new Exception("Column names must be string!");
            }

            string columnName = (string)Value.Value;
            
            return new()
            {
                Type = NodeType.Value,
                Value = NodeValue.Parse(data[columnName])
            };
        }
    }
}
