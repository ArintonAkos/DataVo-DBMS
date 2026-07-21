using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.ValueConversion;

/// <summary>
/// Converts one CLR value type to and from DataVo's typed value representation.
/// Integrations own converters for their external types; Core owns only this Unity-free contract.
/// </summary>
public interface IDataVoValueConverter
{
    /// <summary>The CLR type handled by this converter.</summary>
    Type ValueType { get; }

    /// <summary>Stable identity used to make re-registration of the same converter idempotent.</summary>
    string RegistrationKey { get; }

    /// <summary>Converts a boxed instance of <see cref="ValueType"/> to a typed DataVo cell.</summary>
    CellValue ToCellValue(object value);

    /// <summary>Converts a materialized DataVo result value to an instance of <see cref="ValueType"/>.</summary>
    object FromMaterialized(object? value);
}
