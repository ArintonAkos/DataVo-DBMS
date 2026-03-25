#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PACKAGES_DIR="$ROOT_DIR/artifacts/packages"
TEMP_DIR="$(mktemp -d)"
APP_DIR="$TEMP_DIR/DataVo.PackageSmoke"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

echo "[1/5] Packing DataVo.Core and DataVo.Data..."
dotnet pack "$ROOT_DIR/DataVo.Core/DataVo.Core.csproj" -c Release >/dev/null
dotnet pack "$ROOT_DIR/DataVo.Data/DataVo.Data.csproj" -c Release >/dev/null

echo "[2/5] Verifying package artifacts exist..."
ls "$PACKAGES_DIR"/DataVo.Core.*.nupkg >/dev/null
ls "$PACKAGES_DIR"/DataVo.Data.*.nupkg >/dev/null

echo "[3/5] Creating temporary consumer app..."
dotnet new console -n DataVo.PackageSmoke -o "$APP_DIR" --force >/dev/null

cat > "$APP_DIR/Program.cs" <<'CS'
using System;
using System.Linq;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;
using DataVo.Data;

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

Console.WriteLine("External consumer package validation passed.");
CS

echo "[4/5] Installing local packages from artifacts..."
dotnet add "$APP_DIR/DataVo.PackageSmoke.csproj" package DataVo.Core --source "$PACKAGES_DIR" --prerelease >/dev/null
dotnet add "$APP_DIR/DataVo.PackageSmoke.csproj" package DataVo.Data --source "$PACKAGES_DIR" --prerelease >/dev/null

echo "[5/5] Running consumer app with local packages..."
dotnet run --project "$APP_DIR/DataVo.PackageSmoke.csproj" -c Release >/dev/null

echo "SUCCESS: local DataVo package smoke test passed."
