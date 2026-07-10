#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TEMP_DIR="$(mktemp -d)"
PACKAGES_DIR="$TEMP_DIR/packages"
APP_DIR="$TEMP_DIR/DataVo.PackageSmoke"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

mkdir -p "$PACKAGES_DIR"

echo "[1/6] Packing DataVo.Core and DataVo.Data..."
dotnet pack "$ROOT_DIR/DataVo.Core/DataVo.Core.csproj" \
  -c Release \
  -o "$PACKAGES_DIR" >/dev/null
dotnet pack "$ROOT_DIR/DataVo.Data/DataVo.Data.csproj" \
  -c Release \
  -o "$PACKAGES_DIR" >/dev/null

echo "[2/6] Verifying package artifacts exist..."
CORE_PACKAGE="$(
  find "$PACKAGES_DIR" -maxdepth 1 -type f \
    -name 'DataVo.Core.*.nupkg' -print
)"
DATA_PACKAGE="$(
  find "$PACKAGES_DIR" -maxdepth 1 -type f \
    -name 'DataVo.Data.*.nupkg' -print
)"

if [[ -z "$CORE_PACKAGE" || "$CORE_PACKAGE" == *$'\n'* ]]; then
  echo "Expected exactly one DataVo.Core nupkg, found:" >&2
  printf '%s\n' "${CORE_PACKAGE:-<none>}" >&2
  exit 1
fi

if [[ -z "$DATA_PACKAGE" || "$DATA_PACKAGE" == *$'\n'* ]]; then
  echo "Expected exactly one DataVo.Data nupkg, found:" >&2
  printf '%s\n' "${DATA_PACKAGE:-<none>}" >&2
  exit 1
fi

echo "[3/6] Verifying the public Core target boundary..."
bash "$ROOT_DIR/scripts/assert-core-package-targets.sh" "$CORE_PACKAGE"

echo "[4/6] Creating temporary consumer app..."
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

echo "[5/6] Installing local packages from artifacts..."
dotnet add "$APP_DIR/DataVo.PackageSmoke.csproj" package DataVo.Core --source "$PACKAGES_DIR" --prerelease >/dev/null
dotnet add "$APP_DIR/DataVo.PackageSmoke.csproj" package DataVo.Data --source "$PACKAGES_DIR" --prerelease >/dev/null

echo "[6/6] Running consumer app with local packages..."
dotnet run --project "$APP_DIR/DataVo.PackageSmoke.csproj" -c Release >/dev/null

echo "SUCCESS: local DataVo package smoke test passed."
