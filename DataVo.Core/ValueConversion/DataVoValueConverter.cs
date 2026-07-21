using DataVo.Core.Compat;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.ValueConversion;

/// <summary>
/// Generic adapter used by integrations to register an external CLR value type with a DataVo context.
/// </summary>
public sealed class DataVoValueConverter<T> : IDataVoValueConverter
{
    private readonly Func<T, CellValue> _toCellValue;
    private readonly Func<object?, T> _fromMaterialized;

    /// <summary>Creates a converter for <typeparamref name="T"/>.</summary>
    public DataVoValueConverter(
        string registrationKey,
        Func<T, CellValue> toCellValue,
        Func<object?, T> fromMaterialized)
    {
        if (string.IsNullOrWhiteSpace(registrationKey))
        {
            throw new ArgumentException("Converter registration key is required.", nameof(registrationKey));
        }

        ThrowHelper.ThrowIfNull(toCellValue);
        ThrowHelper.ThrowIfNull(fromMaterialized);

        RegistrationKey = registrationKey;
        _toCellValue = toCellValue;
        _fromMaterialized = fromMaterialized;
    }

    /// <inheritdoc />
    public Type ValueType => typeof(T);

    /// <inheritdoc />
    public string RegistrationKey { get; }

    /// <inheritdoc />
    public CellValue ToCellValue(object value)
    {
        if (value is not T typed)
        {
            throw new ArgumentException(
                $"Converter for '{typeof(T).FullName}' cannot write value of type '{value?.GetType().FullName ?? "null"}'.",
                nameof(value));
        }

        return _toCellValue(typed);
    }

    /// <inheritdoc />
    public object FromMaterialized(object? value) => _fromMaterialized(value)!;
}
