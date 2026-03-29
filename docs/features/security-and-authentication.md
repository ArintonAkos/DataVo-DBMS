# Security and Authentication

This page documents the implemented principal, permission, and session-auth SQL surface.

## Scope

The following command families are supported:

- principal management: `CREATE USER`, `CREATE ROLE`
- permission management: `GRANT`, `REVOKE`
- session authentication: `LOGIN`, `LOGOUT`
- introspection: `SHOW USERS`, `SHOW ROLES`, `SHOW GRANTS`, `SHOW GRANTS FOR USER`, `SHOW GRANTS FOR ROLE`

## Example flow

```sql
CREATE DATABASE SecurityDemo;
USE SecurityDemo;

CREATE USER app_user;
CREATE ROLE app_reader;

GRANT SELECT ON Users TO app_reader;
GRANT ROLE app_reader TO app_user;

SHOW USERS;
SHOW ROLES;
SHOW GRANTS;
SHOW GRANTS FOR USER app_user;
SHOW GRANTS FOR ROLE app_reader;

LOGIN app_user;
LOGOUT;
```

## Command reference

## `CREATE USER`

Creates a user principal.

```sql
CREATE USER analyst;
```

## `CREATE ROLE`

Creates a role principal.

```sql
CREATE ROLE reporting;
```

## `GRANT`

Assigns privileges or role membership.

Examples:

```sql
GRANT SELECT ON Orders TO reporting;
GRANT ROLE reporting TO analyst;
```

## `REVOKE`

Removes privileges or role membership.

Examples:

```sql
REVOKE SELECT ON Orders FROM reporting;
REVOKE ROLE reporting FROM analyst;
```

## `LOGIN` and `LOGOUT`

Changes current session authentication context.

```sql
LOGIN analyst;
LOGOUT;
```

## `SHOW` commands

Use these commands for audit and diagnostics:

```sql
SHOW USERS;
SHOW ROLES;
SHOW GRANTS;
SHOW GRANTS FOR USER analyst;
SHOW GRANTS FOR ROLE reporting;
```

## Operational guidance

- Use roles for permission grouping and assign roles to users.
- Keep direct user grants minimal to simplify revocation and review.
- Capture `SHOW GRANTS` output in release verification checks.

## Related docs

- [Getting Started](./getting-started.md)
- [Schema and DDL](./schema-and-ddl.md)
- [Transactions](./transactions.md)
- [Roadmap and Integrations](./roadmap-and-integrations.md)
