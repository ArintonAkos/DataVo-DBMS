using System.Xml.Linq;
using DataVo.Core.Models.Catalog;

namespace DataVo.Core.Runtime.Catalog;

/// <summary>
/// Reflection-free (Native-AOT-safe) mappers between catalog model objects and their on-disk XML form.
/// Replaces the previous <c>XmlSerializer</c> round-trip used by both <see cref="CatalogStore"/> and the
/// static <see cref="Models.Catalog.Catalog"/>. The element and attribute names reproduce the exact
/// <c>Catalog.xml</c> format the catalog reader navigates, so the on-disk format is preserved
/// (no breaking change) while removing the runtime-code-generating serializer.
/// </summary>
internal static class CatalogXml
{
    public static XElement ToXElement(Database database) =>
        new("Database",
            new XAttribute("DatabaseName", database.DatabaseName),
            new XElement("Tables", (database.Tables ?? []).Select(ToXElement)));

    public static XElement ToXElement(Table table)
    {
        var element = new XElement("Table",
            new XAttribute("TableName", table.TableName),
            new XElement("Structure", (table.Fields ?? []).Select(ToXElement)),
            new XElement("PrimaryKeys",
                (table.PrimaryKeys ?? []).Select(pk => new XElement("PkAttribute", pk))));

        // Empty ForeignKeys/UniqueKeys are omitted entirely (matches the XmlSerializer *Specified pattern).
        if (table.ForeignKeys is { Count: > 0 })
        {
            element.Add(new XElement("ForeignKeys", table.ForeignKeys.Select(ToXElement)));
        }

        if (table.UniqueAttributes is { Count: > 0 })
        {
            element.Add(new XElement("UniqueKeys",
                table.UniqueAttributes.Select(uk => new XElement("UniqueAttribute", uk))));
        }

        element.Add(new XElement("IndexFiles", (table.IndexFiles ?? []).Select(ToXElement)));
        return element;
    }

    public static XElement ToXElement(Field field)
    {
        // Attribute order matches the model declaration order: Type, Name, IsNull, Length, DefaultValue.
        // Defaulted values are omitted exactly as XmlSerializer did via [DefaultValue].
        var element = new XElement("Attribute",
            new XAttribute("Type", field.Type.ToString()),
            new XAttribute("Name", field.Name));

        if (field.IsNull != -1)
        {
            element.SetAttributeValue("IsNull", field.IsNull);
        }

        if (field.Length != 0)
        {
            element.SetAttributeValue("Length", field.Length);
        }

        if (field.DefaultValue is not null)
        {
            element.SetAttributeValue("DefaultValue", field.DefaultValue);
        }

        return element;
    }

    public static XElement ToXElement(ForeignKey foreignKey) =>
        new("ForeignKey",
            new XElement("FkAttribute", foreignKey.AttributeName),
            new XElement("References", (foreignKey.References ?? []).Select(ToXElement)),
            new XElement("OnDeleteAction", foreignKey.OnDeleteAction));

    public static XElement ToXElement(Reference reference) =>
        new("Reference",
            new XElement("RefTable", reference.ReferenceTableName),
            new XElement("RefAttribute", reference.ReferenceAttributeName));

    public static XElement ToXElement(IndexFile indexFile)
    {
        var element = new XElement("IndexFile",
            new XAttribute("IndexName", indexFile.IndexFileName));

        // IndexKind is omitted when the default ("BTREE"), matching the model's [DefaultValue].
        if (!string.Equals(indexFile.IndexKind, "BTREE", StringComparison.Ordinal))
        {
            element.SetAttributeValue("IndexKind", indexFile.IndexKind);
        }

        element.Add(new XElement("IndexAttributes",
            (indexFile.AttributeNames ?? []).Select(name => new XElement("IAttribute", name))));
        return element;
    }

    public static ForeignKey ForeignKeyFromXElement(XElement element) => new()
    {
        AttributeName = element.Element("FkAttribute")!.Value,
        References = element.Element("References")?.Elements("Reference")
            .Select(ReferenceFromXElement).ToList() ?? [],
        OnDeleteAction = element.Element("OnDeleteAction")?.Value ?? "RESTRICT",
    };

    public static Reference ReferenceFromXElement(XElement element) => new()
    {
        ReferenceTableName = element.Element("RefTable")!.Value,
        ReferenceAttributeName = element.Element("RefAttribute")!.Value,
    };

    public static IndexFile IndexFileFromXElement(XElement element) => new()
    {
        IndexFileName = element.Attribute("IndexName")!.Value,
        IndexKind = element.Attribute("IndexKind")?.Value ?? "BTREE",
        AttributeNames = element.Element("IndexAttributes")?.Elements("IAttribute")
            .Select(item => item.Value).ToList() ?? [],
    };
}
