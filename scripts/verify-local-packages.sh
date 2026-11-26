#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TEMP_DIR="$(mktemp -d)"
RUN_ID="$(printf '%s' "${TEMP_DIR##*.}" | tr '[:upper:]' '[:lower:]')"
PACKAGE_VERSION="0.0.0-local.run$RUN_ID"
NUGET_PACKAGES="$TEMP_DIR/nuget-packages"
NUGET_FALLBACK_PACKAGES="${NUGET_FALLBACK_PACKAGES-$HOME/.nuget/packages}"
export NUGET_PACKAGES NUGET_FALLBACK_PACKAGES
PACKAGES_DIR="$TEMP_DIR/packages"
APP_DIR="$TEMP_DIR/DataVo.PackageSmoke"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

mkdir -p "$PACKAGES_DIR" "$NUGET_PACKAGES"

readonly PROJECTS=(
  DataVo.Core
  DataVo.Data
  DataVo.EntityFrameworkCore
  DataVo.Generators
  DataVo.VectorData
)

echo "[1/5] Packing the five publishable packages..."
for project in "${PROJECTS[@]}"; do
  dotnet pack "$ROOT_DIR/$project/$project.csproj" \
    -c Release \
    -p:PackageVersion="$PACKAGE_VERSION" \
    -o "$PACKAGES_DIR" >/dev/null
done

echo "[2/5] Verifying every package exists with its expected target layout..."
bash "$ROOT_DIR/scripts/assert-release-package-targets.sh" "$PACKAGES_DIR"

echo "[3/5] Creating temporary consumer app..."
dotnet new console -n DataVo.PackageSmoke -o "$APP_DIR" --force >/dev/null

cat > "$APP_DIR/Program.cs" <<'CS'
using System;
using System.IO;
using System.Linq;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;
using DataVo.Data;
using DataVo.EntityFrameworkCore;
using DataVo.VectorData;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.VectorData;

using (var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory }))
{
  ctx.Execute("CREATE DATABASE DemoCore");
  ctx.Execute("USE DemoCore");
  ctx.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(50))");
  ctx.Execute("INSERT INTO Users VALUES (1, 'Alice')");

  var select = ctx.Execute("SELECT Name FROM Users WHERE Id = 1").Last();
  if (select.IsError || select.Data.Count != 1 || !string.Equals(select.Data[0]["Name"]?.ToString(), "Alice", StringComparison.Ordinal))
  {
    throw new Exception("DataVo.Core validation failed in external consumer app.");
  }
}

using (var connection = new DataVoConnection("StorageMode=InMemory;DataSource=DemoAdo"))
{
  connection.Open();

  using var command = connection.CreateCommand();
  command.CommandText = "CREATE TABLE People (Id INT PRIMARY KEY, Name VARCHAR(50));";
  command.ExecuteNonQuery();

  command.CommandText = "INSERT INTO People VALUES (1, 'Bob');";
  command.ExecuteNonQuery();

  command.CommandText = "SELECT Name FROM People WHERE Id = 1;";
  using var reader = command.ExecuteReader();
  if (!reader.Read() || !string.Equals(reader[0]?.ToString(), "Bob", StringComparison.Ordinal))
  {
    throw new Exception("DataVo.Data validation failed in external consumer app.");
  }
}

using (var store = new DataVoVectorStore(new DataVoConfig { StorageMode = StorageMode.InMemory }))
{
  VectorStoreCollection<int, Doc> docs = store.GetCollection<int, Doc>("docs");
  await docs.EnsureCollectionExistsAsync();
  await docs.UpsertAsync([
    new Doc { Id = 1, Title = "east", Vector = new float[] { 1f, 0f } },
    new Doc { Id = 2, Title = "north", Vector = new float[] { 0f, 1f } },
  ]);

  var hits = await docs.SearchAsync(new ReadOnlyMemory<float>(new float[] { 0.9f, 0.1f }), top: 1).ToListAsync();
  if (hits.Count != 1 || hits[0].Record.Id != 1)
  {
    throw new Exception("DataVo.VectorData validation failed in external consumer app.");
  }
}

string efDataSource = $"datavo_package_smoke_ef_{Guid.NewGuid():N}";
string efConnectionString = $"StorageMode=Lsm;DataSource={efDataSource}";
try
{
  var efOptions = new DbContextOptionsBuilder<SmokeContext>()
    .UseInMemoryDatabase(efDataSource)
    .UseDataVo(efConnectionString)
    .Options;
  using (var ef = new SmokeContext(efOptions))
  {
    ef.Database.EnsureDataVoCreated();
    ef.Add(new Ticket { Id = 1, Title = "ship" });
    if (ef.SaveChangesToDataVo() != 1)
    {
      throw new Exception("DataVo.EntityFrameworkCore validation failed in external consumer app.");
    }
  }

  using var efConnection = new DataVoConnection(efConnectionString);
  efConnection.Open();
  using var efCommand = efConnection.CreateCommand();
  efCommand.CommandText = "SELECT Title FROM Tickets WHERE Id = 1;";
  using var efReader = efCommand.ExecuteReader();
  if (!efReader.Read() || !string.Equals(efReader[0]?.ToString(), "ship", StringComparison.Ordinal))
  {
    throw new Exception("DataVo.EntityFrameworkCore row was not persisted in external consumer app.");
  }
}
finally
{
  string efPath = Path.Combine(Directory.GetCurrentDirectory(), efDataSource);
  if (Directory.Exists(efPath))
  {
    Directory.Delete(efPath, recursive: true);
  }
}

Console.WriteLine("External consumer package validation passed.");

public sealed class Doc
{
  [VectorStoreKey]
  public int Id { get; set; }

  [VectorStoreData]
  public string Title { get; set; } = "";

  [VectorStoreVector(2, DistanceFunction = DistanceFunction.CosineSimilarity, IndexKind = IndexKind.Flat)]
  public ReadOnlyMemory<float> Vector { get; set; }
}

public sealed class Ticket
{
  public int Id { get; set; }
  public string Title { get; set; } = "";
}

public sealed class SmokeContext(DbContextOptions<SmokeContext> options) : DbContext(options)
{
  protected override void OnModelCreating(ModelBuilder modelBuilder)
  {
    modelBuilder.Entity<Ticket>(entity =>
    {
      entity.ToTable("Tickets");
      entity.HasKey(static ticket => ticket.Id);
      entity.Property(static ticket => ticket.Title).HasMaxLength(50);
    });
  }
}
CS

echo "[4/5] Installing local packages from artifacts..."
# The local artifacts feed supplies the DataVo candidates; nuget.org supplies their third-party dependencies.
cat > "$APP_DIR/nuget.config" <<XML
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="datavo-local" value="$PACKAGES_DIR" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
XML
for project in "${PROJECTS[@]}"; do
  dotnet add "$APP_DIR/DataVo.PackageSmoke.csproj" package "$project" --version "$PACKAGE_VERSION" --no-restore >/dev/null
done
# The EF Core bridge tracks entities through an EF provider; the in-memory one keeps the smoke self-contained.
dotnet add "$APP_DIR/DataVo.PackageSmoke.csproj" package Microsoft.EntityFrameworkCore.InMemory --version 10.0.0 --no-restore >/dev/null
dotnet restore "$APP_DIR/DataVo.PackageSmoke.csproj" >/dev/null

ASSETS_FILE="$APP_DIR/obj/project.assets.json"
for project in "${PROJECTS[@]}"; do
  if ! grep -Fq "\"$project/$PACKAGE_VERSION\":" "$ASSETS_FILE"; then
    echo "Expected consumer assets to resolve $project/$PACKAGE_VERSION." >&2
    exit 1
  fi
done

# The source generator must reach the consumer as an analyzer, not as a referenced library.
if ! grep -Fq "analyzers/dotnet/cs/DataVo.Generators.dll" "$ASSETS_FILE"; then
  echo "Expected consumer assets to load DataVo.Generators as an analyzer." >&2
  exit 1
fi

echo "[5/5] Running consumer app with local packages..."
dotnet run --project "$APP_DIR/DataVo.PackageSmoke.csproj" -c Release

echo "SUCCESS: local DataVo package smoke test passed."
