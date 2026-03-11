namespace DataVo.Core.Enums;

[Serializable]
/// <summary>
/// Represents the primitive SQL data types currently supported by the engine catalog.
/// </summary>
public enum DataTypes
{
    /// <summary>32-bit integer values.</summary>
    Int,

    /// <summary>Floating-point numeric values.</summary>
    Float,

    /// <summary>Boolean values stored as a bit.</summary>
    Bit,

    /// <summary>Date-only values.</summary>
    Date,

    /// <summary>Variable-length UTF-16 string values.</summary>
    Varchar,
}