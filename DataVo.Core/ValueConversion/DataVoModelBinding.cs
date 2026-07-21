using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using DataVo.Core.Compat;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.ValueConversion;

/// <summary>
/// Caches reflection metadata for the convention-based model APIs owned by one <see cref="DataVoContext"/>.
/// </summary>
internal sealed class DataVoModelBinding
{
    private readonly Dictionary<Type, ReadBinding> _readBindings = [];
    private readonly Dictionary<WriteBindingKey, WriteBinding> _writeBindings = [];

    public T Map<[DynamicallyAccessedMembers(ModelMembers)] T>(
        IReadOnlyDictionary<string, object?> row,
        DataVoValueConverterRegistry converters)
    {
        ThrowHelper.ThrowIfNull(row);
        ThrowHelper.ThrowIfNull(converters);

        ReadBinding binding = GetReadBinding(typeof(T));
        object model = binding.CreateInstance();

        foreach ((string columnName, object? value) in row)
        {
            if (!binding.TryGetWritableProperty(columnName, out PropertyInfo? property) || property is null)
            {
                continue;
            }

            object? converted = converters.ConvertMaterialized(value, property.PropertyType);
            property.SetValue(model, converted);
        }

        return (T)model;
    }

    public WriteBinding GetWriteBinding(
        [DynamicallyAccessedMembers(ModelMembers)] Type modelType,
        string databaseName,
        string tableName,
        int schemaVersion,
        IReadOnlyList<Column> columns)
    {
        ThrowHelper.ThrowIfNull(modelType);
        ThrowHelper.ThrowIfNull(databaseName);
        ThrowHelper.ThrowIfNull(tableName);
        ThrowHelper.ThrowIfNull(columns);

        var key = new WriteBindingKey(modelType, databaseName, tableName, schemaVersion);
        if (_writeBindings.TryGetValue(key, out WriteBinding? cached))
        {
            return cached;
        }

        WriteBinding binding = BuildWriteBinding(modelType, columns);
        _writeBindings.Add(key, binding);
        return binding;
    }

    private ReadBinding GetReadBinding([DynamicallyAccessedMembers(ModelMembers)] Type modelType)
    {
        if (_readBindings.TryGetValue(modelType, out ReadBinding? cached))
        {
            return cached;
        }

        ReadBinding binding = BuildReadBinding(modelType);
        _readBindings.Add(modelType, binding);
        return binding;
    }

    private static ReadBinding BuildReadBinding([DynamicallyAccessedMembers(ModelMembers)] Type modelType)
    {
        ConstructorInfo constructor = RequirePublicParameterlessConstructor(modelType);
        var propertiesByName = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);

        foreach (PropertyInfo property in modelType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            MethodInfo? setter = property.SetMethod;
            if (setter is null || !setter.IsPublic || setter.IsStatic || property.GetIndexParameters().Length != 0)
            {
                continue;
            }

            propertiesByName[property.Name] = property;
        }

        return new ReadBinding(constructor, propertiesByName);
    }

    private static WriteBinding BuildWriteBinding(
        [DynamicallyAccessedMembers(ModelMembers)] Type modelType,
        IReadOnlyList<Column> columns)
    {
        RequirePublicParameterlessConstructor(modelType);
        var propertiesByName = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);

        foreach (PropertyInfo property in modelType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            MethodInfo? getter = property.GetMethod;
            if (getter is null || !getter.IsPublic || getter.IsStatic || property.GetIndexParameters().Length != 0)
            {
                continue;
            }

            propertiesByName[property.Name] = property;
        }

        var sourceProperties = new PropertyInfo[columns.Count];
        var columnNames = new string[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            Column column = columns[i];
            if (!propertiesByName.TryGetValue(column.Name, out PropertyInfo? property))
            {
                throw new InvalidOperationException(
                    $"Model type '{modelType.FullName}' does not expose a readable public property for catalog column '{column.Name}'.");
            }

            sourceProperties[i] = property;
            columnNames[i] = column.Name;
        }

        return new WriteBinding(new ReactiveRowSchema(columnNames), sourceProperties);
    }

    private static ConstructorInfo RequirePublicParameterlessConstructor(
        [DynamicallyAccessedMembers(ModelMembers)] Type modelType)
    {
        ConstructorInfo? constructor = modelType.GetConstructor(Type.EmptyTypes);
        if (constructor is null || !constructor.IsPublic)
        {
            throw new InvalidOperationException(
                $"Model type '{modelType.FullName}' must expose a public parameterless constructor.");
        }

        return constructor;
    }

    private readonly record struct WriteBindingKey(Type ModelType, string DatabaseName, string TableName, int SchemaVersion);

    private const DynamicallyAccessedMemberTypes ModelMembers =
        DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties;

    internal sealed class WriteBinding
    {
        private readonly PropertyInfo[] _sourceProperties;

        public WriteBinding(ReactiveRowSchema schema, PropertyInfo[] sourceProperties)
        {
            Schema = schema;
            _sourceProperties = sourceProperties;
        }

        public ReactiveRowSchema Schema { get; }

        public CellValue[] CreateCells(object entity, DataVoValueConverterRegistry converters)
        {
            ThrowHelper.ThrowIfNull(entity);
            ThrowHelper.ThrowIfNull(converters);

            var cells = new CellValue[_sourceProperties.Length];
            for (int i = 0; i < _sourceProperties.Length; i++)
            {
                cells[i] = converters.ConvertToCellValue(_sourceProperties[i].GetValue(entity));
            }

            return cells;
        }
    }

    private sealed class ReadBinding(ConstructorInfo constructor, Dictionary<string, PropertyInfo> writableProperties)
    {
        public object CreateInstance() => constructor.Invoke(null)!;

        public bool TryGetWritableProperty(string columnName, out PropertyInfo? property) =>
            writableProperties.TryGetValue(columnName, out property);
    }
}
