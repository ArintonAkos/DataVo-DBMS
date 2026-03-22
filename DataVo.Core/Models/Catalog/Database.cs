using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog;

/// <summary>
/// Represents a database within the DBMS catalog.
/// Contains the collection of related tables that reside within it.
/// </summary>
/// <example>
/// <code>
/// var db = new Database { DatabaseName = "test_db", Tables = new List&lt;Table&gt;() };
/// </code>
/// </example>
[Serializable]
[XmlRoot("Database")]
public class Database
{
    /// <summary>
    /// Gets or sets the name of the database.
    /// </summary>
    /// <example>
    /// "customer_db"
    /// </example>
    [XmlAttribute] public string DatabaseName { get; set; } = null!;

    /// <summary>
    /// Gets or sets the collection of tables belonging to this database.
    /// </summary>
    /// <example>
    /// A list containing 'Users' and 'Orders' tables.
    /// </example>
    [XmlArray("Tables")]
    [XmlArrayItem("Table")]
    public List<Table> Tables { get; set; } = null!;
}