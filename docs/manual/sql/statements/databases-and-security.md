---
title: Databases & Security
description: Manage databases, inspect schemas, and configure built-in RBAC users, roles, grants, and sessions in DataVo v0.4.
---

# Databases & Security

DataVo provides SQL statements for managing databases, switching session context, inspecting catalog metadata, and administering its built-in role-based access control (RBAC) system.

## Syntax

### Databases & Catalog Inspection

```sql
CREATE DATABASE [IF NOT EXISTS] <database_name>;
USE <database_name>;
DROP DATABASE [IF EXISTS] <database_name>;

SHOW DATABASES;
SHOW TABLES;
DESCRIBE <table_name>;
```

### Users, Roles, Grants & Sessions

```sql
CREATE USER <username> IDENTIFIED [BY | PASSWORD] '<password>' [ROLE <role_name>];
DROP USER <username>;

CREATE ROLE <role_name>;
DROP ROLE <role_name>;

GRANT <perm1> [, <perm2>, ...] TO [ROLE] <user_or_role>;
GRANT ROLE <role_name> TO <username>;

REVOKE <perm1> [, <perm2>, ...] FROM [ROLE] <user_or_role>;
REVOKE ROLE <role_name> FROM <username>;

LOGIN <username> IDENTIFIED [BY | PASSWORD] '<password>';
LOGOUT;

SHOW USERS;
SHOW ROLES;
SHOW GRANTS [FOR USER <username> | FOR ROLE <role_name>];
```

::: info DataVo Extension
DataVo's built-in RBAC statements (`LOGIN`, `LOGOUT`, `CREATE USER ... ROLE ...`, and engine-scoped `GRANT <perm> TO [ROLE] <target>`) form a lightweight embedded security model rather than a PostgreSQL role hierarchy.
:::

## Database & Session Statements

| Statement | Description |
| --- | --- |
| `CREATE DATABASE [IF NOT EXISTS] <db>` | Creates a new database in the engine catalog. With `IF NOT EXISTS`, succeeds as a no-op if `<db>` already exists. |
| `USE <db>` | Switches the active database for the current session. |
| `DROP DATABASE [IF EXISTS] <db>` | Permanently deletes `<db>`, all of its tables, and all of its indexes. With `IF EXISTS`, succeeds as a no-op if `<db>` does not exist. |
| `SHOW DATABASES` | Lists all databases registered in the engine catalog. |
| `SHOW TABLES` | Lists all tables in the active database. |
| `DESCRIBE <table>` | Displays column names, types, lengths, key flags, and defaults for `<table>` in the active database. |
| `GO` | Batch separator accepted as a no-op for SQL script compatibility. |

### Database Lifecycle & Inspection Example

```sql
CREATE DATABASE IF NOT EXISTS Analytics;
USE Analytics;

CREATE TABLE events (
    id INT PRIMARY KEY,
    kind VARCHAR(40) NOT NULL,
    occurred_on DATE
);

SHOW DATABASES;
SHOW TABLES;
DESCRIBE events;

DROP DATABASE IF EXISTS Analytics;
```

## Users & Roles

Create roles to group permissions, then assign roles when creating users or later via `GRANT ROLE`:

```sql
CREATE ROLE analyst;
CREATE ROLE writer;

CREATE USER alice IDENTIFIED BY 's3cr3t' ROLE analyst;
CREATE USER bob IDENTIFIED PASSWORD 'another_secret';

SHOW ROLES;
SHOW USERS;
```

Both `IDENTIFIED BY '<password>'` and `IDENTIFIED PASSWORD '<password>'` (or `IDENTIFIED '<password>'`) are accepted by the parser.

To remove users or roles:

```sql
DROP USER bob;
DROP ROLE writer;
```

## Permissions (`GRANT` & `REVOKE`)

DataVo authorizes operations using six engine-level permission tokens. Permissions can be granted directly to a user or to a role (with the `ROLE` keyword before the target).

### Supported Permission Tokens

| Permission | Accepted Aliases | Operations Covered |
| --- | --- | --- |
| `READ` | `READDATA` | `SELECT`, `EXPLAIN`, and read-only inspection queries (`SHOW`, `DESCRIBE`) |
| `WRITE` | `WRITEDATA` | `INSERT`, `UPDATE`, `DELETE`, and `VACUUM` |
| `SCHEMA` | `MANAGESCHEMA` | `CREATE`, `ALTER`, and `DROP` for databases, tables, and indexes |
| `TRANSACTION` | `TRANSACTIONS`, `MANAGETRANSACTIONS` | `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO`, `RELEASE` |
| `SECURITY` | `MANAGESECURITY` | `CREATE`/`DROP` for users and roles, `GRANT`, `REVOKE`, `SHOW USERS`/`ROLES`/`GRANTS` |
| `ADMIN` | — | Full administrative privileges across every operation |

::: warning No Object-Level `ON <table>` Clause
DataVo permissions are granted at the engine/principal level: there is **no `ON <table>` or `ON <database>.*` clause** in `GRANT` or `REVOKE`. Adding an `ON ...` clause produces a `ParserException`.
:::

### Granting, Revoking & Inspecting Privileges

```sql
-- Grant permissions to a role
GRANT READ TO ROLE analyst;
GRANT READ, WRITE, TRANSACTION TO ROLE writer;

-- Grant a role or direct permissions to a user
GRANT ROLE writer TO bob;
GRANT SCHEMA TO alice;

-- Inspect grants globally or for a specific principal
SHOW GRANTS;
SHOW GRANTS FOR USER alice;
SHOW GRANTS FOR ROLE analyst;

-- Revoke permissions or a role
REVOKE SCHEMA FROM alice;
REVOKE ROLE writer FROM bob;
REVOKE WRITE FROM ROLE writer;
```

## Session Authentication (`LOGIN` & `LOGOUT`)

Switch the authenticated user bound to the current session using `LOGIN`, and clear the session's authenticated principal using `LOGOUT`:

```sql
LOGIN alice IDENTIFIED BY 's3cr3t';

SELECT id, kind FROM events;

LOGOUT;
```

::: tip
Unauthorized statements throw an `AuthorizationException` when engine authentication is enabled. Configure default authentication and RBAC behavior via `DataVoConfig` (see [Configuration Reference](/manual/reference/configuration)).
:::

## See Also

- [CREATE TABLE](/manual/sql/statements/create-table): Defining tables inside a database
- [Transactions](/manual/sql/statements/transactions): Transaction control statements covered by the `TRANSACTION` permission
- [Configuration Reference](/manual/reference/configuration): Engine authentication and security configuration options
