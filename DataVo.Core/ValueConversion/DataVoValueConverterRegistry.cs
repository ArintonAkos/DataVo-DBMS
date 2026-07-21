using DataVo.Core.Compat;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.ValueConversion;

/// <summary>
/// Holds the external value converters configured for one <see cref="DataVoContext"/>.
/// </summary>
internal sealed class DataVoValueConverterRegistry
{
    private readonly Dictionary<Type, IDataVoValueConverter> _converters = [];

    public void RegisterRange(IReadOnlyList<IDataVoValueConverter> converters)
    {
        ThrowHelper.ThrowIfNull(converters);

        var incoming = new Dictionary<Type, IDataVoValueConverter>();
        for (int i = 0; i < converters.Count; i++)
        {
            IDataVoValueConverter converter = converters[i]
                ?? throw new ArgumentException("Converter groups cannot contain null entries.", nameof(converters));

            if (string.IsNullOrWhiteSpace(converter.RegistrationKey))
            {
                throw new ArgumentException("Converter registration keys cannot be null or whitespace.", nameof(converters));
            }

            if (!incoming.TryAdd(converter.ValueType, converter))
            {
                throw new ArgumentException(
                    $"Converter group contains more than one converter for '{converter.ValueType.FullName}'.",
                    nameof(converters));
            }
        }

        foreach ((Type valueType, IDataVoValueConverter converter) in incoming)
        {
            if (_converters.TryGetValue(valueType, out IDataVoValueConverter? existing)
                && !string.Equals(existing.RegistrationKey, converter.RegistrationKey, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"A different converter is already registered for '{valueType.FullName}'.");
            }
        }

        foreach ((Type valueType, IDataVoValueConverter converter) in incoming)
        {
            _converters.TryAdd(valueType, converter);
        }
    }

    public T ConvertMaterialized<T>(object? value)
    {
        if (value is null)
        {
            if (!typeof(T).IsValueType || Nullable.GetUnderlyingType(typeof(T)) is not null)
            {
                return default!;
            }

            throw new InvalidOperationException($"Cannot read SQL NULL as non-nullable '{typeof(T).FullName}'.");
        }

        if (value is T direct)
        {
            return direct;
        }

        if (!_converters.TryGetValue(typeof(T), out IDataVoValueConverter? converter))
        {
            throw new NotSupportedException($"No DataVo value converter is registered for '{typeof(T).FullName}'.");
        }

        if (converter is DataVoValueConverter<T> typedConverter)
        {
            return typedConverter.FromMaterializedTyped(value);
        }

        object converted = converter.FromMaterialized(value);
        if (converted is not T result)
        {
            throw new InvalidOperationException(
                $"Converter '{converter.RegistrationKey}' returned '{converted?.GetType().FullName ?? "null"}' for '{typeof(T).FullName}'.");
        }

        return result;
    }

    internal object? ConvertMaterialized(object? value, Type targetType)
    {
        ThrowHelper.ThrowIfNull(targetType);

        if (value is null)
        {
            if (!targetType.IsValueType || Nullable.GetUnderlyingType(targetType) is not null)
            {
                return null;
            }

            throw new InvalidOperationException($"Cannot read SQL NULL as non-nullable '{targetType.FullName}'.");
        }

        if (targetType.IsInstanceOfType(value))
        {
            return value;
        }

        if (!_converters.TryGetValue(targetType, out IDataVoValueConverter? converter))
        {
            throw new NotSupportedException($"No DataVo value converter is registered for '{targetType.FullName}'.");
        }

        object converted = converter.FromMaterialized(value);
        if (!targetType.IsInstanceOfType(converted))
        {
            throw new InvalidOperationException(
                $"Converter '{converter.RegistrationKey}' returned '{converted?.GetType().FullName ?? "null"}' for '{targetType.FullName}'.");
        }

        return converted;
    }

    internal CellValue ConvertToCellValue(object? value)
    {
        if (value is null)
        {
            return CellValue.Null;
        }

        Type valueType = value.GetType();
        return _converters.TryGetValue(valueType, out IDataVoValueConverter? converter)
            ? converter.ToCellValue(value)
            : CellValue.From(value);
    }

    internal object? BindForLegacyWrite(object? value)
    {
        if (value is null || value is DataVoBoundValue)
        {
            return value;
        }

        return _converters.TryGetValue(value.GetType(), out IDataVoValueConverter? converter)
            ? new DataVoBoundValue(converter.ToCellValue(value))
            : value;
    }
}
