using System.Xml.Linq;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Storage;

/// <summary>
/// Oracle for the on-disk Catalog.xml format. Target 1 of the Native AOT cleanup replaces the
/// XmlSerializer-based catalog persistence with reflection-free System.Xml.Linq mappers; the element and
/// attribute names the catalog reader navigates by must be preserved exactly. These assertions pass with
/// the original XmlSerializer and must stay green after the swap.
/// </summary>
public class CatalogXmlFormatTests
{
    private static string NewTempDir() =>
        Path.Combine(Path.GetTempPath(), "catxml_" + Guid.NewGuid().ToString("N"));

    [Fact]
    public void Catalog_DatabaseTableFieldsPk_ProduceExpectedXmlShape()
    {
        string dir = NewTempDir();
        try
        {
            using (var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = dir }))
            {
                foreach (string sql in new[]
                {
                    "CREATE DATABASE DbA", "USE DbA",
                    "CREATE TABLE T1 (Id INT PRIMARY KEY, Name VARCHAR(20))",
                })
                {
                    Assert.False(ctx.Execute(sql).Last().IsError, sql);
                }
            }

            XDocument doc = XDocument.Parse(File.ReadAllText(Path.Combine(dir, "Catalog.xml")));

            Assert.Equal("Databases", doc.Root!.Name.LocalName);
            XElement db = Assert.Single(doc.Root!.Elements("Database"));
            Assert.Equal("DbA", db.Attribute("DatabaseName")!.Value);

            XElement table = Assert.Single(db.Element("Tables")!.Elements("Table"));
            Assert.Equal("T1", table.Attribute("TableName")!.Value);

            List<XElement> attributes = table.Element("Structure")!.Elements("Attribute").ToList();
            Assert.Equal(2, attributes.Count);
            Assert.Contains(attributes, a => a.Attribute("Name")!.Value == "Id" && a.Attribute("Type")!.Value == "Int");
            Assert.Contains(attributes, a => a.Attribute("Name")!.Value == "Name" && a.Attribute("Type")!.Value == "Varchar");

            XElement pk = Assert.Single(table.Element("PrimaryKeys")!.Elements("PkAttribute"));
            Assert.Equal("Id", pk.Value);
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Catalog_RoundTripsThroughDisk_ReloadsDatabaseAndTable()
    {
        string dir = NewTempDir();
        try
        {
            using (var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = dir }))
            {
                foreach (string sql in new[]
                {
                    "CREATE DATABASE DbB", "USE DbB",
                    "CREATE TABLE Parent (Id INT PRIMARY KEY, Code VARCHAR(10))",
                    "CREATE TABLE Child (Id INT PRIMARY KEY, ParentId INT, FOREIGN KEY (ParentId) REFERENCES Parent(Id))",
                    "INSERT INTO Parent VALUES (1, 'a')",
                })
                {
                    Assert.False(ctx.Execute(sql).Last().IsError, sql);
                }
            }

            // A fresh context over the same directory must reload the catalog + data written above through
            // the new reflection-free mappers, proving the persisted XML is correct and navigable.
            using (var reopened = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = dir }))
            {
                Assert.False(reopened.Execute("USE DbB").Last().IsError);

                // Schema + row data reloaded from the my-written Catalog.xml + table files.
                var select = reopened.Execute("SELECT Code FROM Parent WHERE Id = 1").Last();
                Assert.False(select.IsError);
                Assert.Single(select.Data);
                Assert.Equal("a", select.Data[0]["Code"]?.ToString());

                // A valid child insert reads the reloaded FK metadata (exercises ForeignKeyFromXElement)
                // and the Child table schema; it must succeed.
                Assert.False(reopened.Execute("INSERT INTO Child VALUES (10, 1)").Last().IsError);
            }
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }
}
