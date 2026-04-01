using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SemanticInventory.Backend.Migrations
{
    /// <inheritdoc />
    public partial class AddFullEmbeddingVectors : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "Dimensions",
                table: "ItemEmbeddings",
                type: "INTEGER",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<string>(
                name: "VectorJson",
                table: "ItemEmbeddings",
                type: "TEXT",
                nullable: false,
                defaultValue: "[]");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "Dimensions",
                table: "ItemEmbeddings");

            migrationBuilder.DropColumn(
                name: "VectorJson",
                table: "ItemEmbeddings");
        }
    }
}
