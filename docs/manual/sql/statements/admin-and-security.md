---
title: Admin & Security Statements
description: Session commands, schema inspection, and built-in RBAC user/role/grant statements in DataVo v0.4.
---

# Admin & Security Statements

DataVo includes session and schema-inspection commands (`USE`, `GO`, `SHOW`, `DESCRIBE`) alongside a lightweight built-in role-based access control (RBAC) system for managing users, roles, and permissions.

```sql
-- Session & inspection
USE App;
SHOW TABLES;
DESCRIBE players;

-- RBAC setup
CREATE ROLE analyst;
GRANT READ TO ROLE analyst;
CREATE USER alice IDENTIFIED BY 's3cr3t' ROLE analyst;
LOGIN alice IDENTIFIED BY 's3cr3t';
```

## Session & Schema Inspection

| Statement | Description |
| --- | --- |
| `USE <database>` | Switches the active database for the current session. |
| `GO` | Batch separator (no-op statement accepted for SQL script compatibility). |
| `SHOW DATABASES` | Lists all databases registered in the engine catalog. |
| `SHOW TABLES` | Lists all tables in the active database. |
| `DESCRIBE <table>` | Displays column names, types, lengths, keys, and defaults for `<table>`. |

## RBAC & Security Statements

DataVo's security model is a standalone embedded RBAC system (not a PostgreSQL role hierarchy).

| Statement | Syntax |
| --- | --- |
| `CREATE USER` | `CREATE USER <user> IDENTIFIED [BY \| PASSWORD] '<password>' [ROLE <role>]` |
| `DROP USER` | `DROP USER <user>` |
| `CREATE ROLE` | `CREATE ROLE <role>` |
| `DROP ROLE` | `DROP ROLE <role>` |
| `GRANT` (permissions) | `GRANT <perm1> [, <perm2>, ...] TO [ROLE] <user_or_role>` |
| `GRANT` (role) | `GRANT ROLE <role> TO <user>` |
| `REVOKE` (permissions) | `REVOKE <perm1> [, <perm2>, ...] FROM [ROLE] <user_or_role>` |
| `REVOKE` (role) | `REVOKE ROLE <role> FROM <user>` |
| `LOGIN` | `LOGIN <user> IDENTIFIED [BY \| PASSWORD] '<password>'` |
| `LOGOUT` | `LOGOUT` |
| `SHOW USERS` | `SHOW USERS` |
| `SHOW ROLES` | `SHOW ROLES` |
| `SHOW GRANTS` | `SHOW GRANTS [FOR USER <user> \| FOR ROLE <role>]` |

### Supported Permission Tokens

| Permission | Accepted Aliases | Scope |
| --- | --- | --- |
| `READ` | `READDATA` | `SELECT`, `EXPLAIN`, and read-only inspection queries |
| `WRITE` | `WRITEDATA` | `INSERT`, `UPDATE`, `DELETE`, `VACUUM` |
| `SCHEMA` | `MANAGESCHEMA` | `CREATE`/`ALTER`/`DROP` for databases, tables, and indexes |
| `TRANSACTION` | `TRANSACTIONS`, `MANAGETRANSACTIONS` | `BEGIN`, `COMMIT`, `ROLLBACK`, and savepoints |
| `SECURITY` | `MANAGESECURITY` | User, role, and grant management |
| `ADMIN` | — | Full administrative privileges across all operations |

## See Also
- [DDL Statements](/manual/sql/statements/ddl) — Creating and dropping databases and tables
- [Configuration Reference](/manual/reference/configuration) — Engine authentication and security configuration options
- [Error Handling](/manual/reference/error-handling) — `AuthorizationException` and DataVo error codes
