using DataVo.Data;
using DataVo.EntityFrameworkCore;
using DataVo.Tests.BrowserParity;
using Microsoft.EntityFrameworkCore;
using System.IO;

namespace DataVo.Tests.EntityFramework;

[BrowserTranslateIgnore("EntityFramework provider-bridge tests rely on EF runtime semantics and are validated in .NET lane.")]
public class DataVoEfSchemaTests
{
    [Fact]
    public void GenerateDataVoCreateScript_MapsPrimitiveColumnsAndRelationship()
    {
        using var context = CreateContext();

        string script = context.GenerateDataVoCreateScript();

        Assert.Contains("CREATE TABLE IF NOT EXISTS Blogs", script, StringComparison.Ordinal);
        Assert.Contains("Id INT PRIMARY KEY", script, StringComparison.Ordinal);
        Assert.Contains("Name VARCHAR(120)", script, StringComparison.Ordinal);
        Assert.Contains("IsPublic BIT", script, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS Posts", script, StringComparison.Ordinal);
        Assert.Contains("BlogId INT REFERENCES Blogs(Id)", script, StringComparison.Ordinal);
        Assert.Contains("PublishedOn DATE", script, StringComparison.Ordinal);
        Assert.Contains("Rating FLOAT DEFAULT 0", script, StringComparison.Ordinal);
    }

    [Fact]
    public void EnsureDataVoCreated_CreatesTablesUsableByEngine()
    {
        string databaseName = $"datavo_ef_{Guid.NewGuid():N}";
        string databasePath = Path.Combine(Directory.GetCurrentDirectory(), databaseName);

        try
        {
            using (var context = CreateContext())
            {
                context.EnsureDataVoCreated($"StorageMode=Disk;DataSource={databaseName}");
            }

            using var connection = new DataVoConnection($"StorageMode=Disk;DataSource={databaseName}");
            connection.Open();

            using (var insertBlog = connection.CreateCommand())
            {
                insertBlog.CommandText = "INSERT INTO Blogs (Id, Name, IsPublic) VALUES (1, 'DataVo', true);";
                insertBlog.ExecuteNonQuery();
            }

            using (var insertPost = connection.CreateCommand())
            {
                insertPost.CommandText = "INSERT INTO Posts (Id, BlogId, Title, PublishedOn, Rating) VALUES (10, 1, 'Hello', '2026-03-13', 4.5);";
                insertPost.ExecuteNonQuery();
            }

            using var select = connection.CreateCommand();
            select.CommandText = "SELECT * FROM Posts WHERE BlogId = 1;";
            using var reader = select.ExecuteReader();

            Assert.True(reader.Read());
            Assert.Equal("Hello", reader["Title"]?.ToString());
            Assert.Equal(1, Convert.ToInt32(reader["BlogId"]));
        }
        finally
        {
            if (Directory.Exists(databasePath))
            {
                Directory.Delete(databasePath, recursive: true);
            }
        }
    }

    private static BlogContext CreateContext()
    {
        return new BlogContext(
            new DbContextOptionsBuilder<BlogContext>()
                .UseInMemoryDatabase($"ef_model_{Guid.NewGuid():N}")
                .Options);
    }

    private sealed class BlogContext(DbContextOptions<BlogContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Blog>(entity =>
            {
                entity.ToTable("Blogs");
                entity.HasKey(static blog => blog.Id);
                entity.Property(static blog => blog.Name).HasMaxLength(120);
            });

            modelBuilder.Entity<Post>(entity =>
            {
                entity.ToTable("Posts");
                entity.HasKey(static post => post.Id);
                entity.Property(static post => post.Title).HasMaxLength(200);
                entity.Property(static post => post.Rating).HasDefaultValue(0);
                entity.HasOne(static post => post.Blog)
                    .WithMany(static blog => blog.Posts)
                    .HasForeignKey(static post => post.BlogId);
            });
        }
    }

    private sealed class Blog
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsPublic { get; set; }
        public List<Post> Posts { get; set; } = [];
    }

    private sealed class Post
    {
        public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = string.Empty;
        public DateTime PublishedOn { get; set; }
        public double Rating { get; set; }
        public Blog? Blog { get; set; }
    }
}
