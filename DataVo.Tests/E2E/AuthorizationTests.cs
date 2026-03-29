using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Security;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class AuthorizationTests
{
    [Fact]
    public void Authorization_ReadOnlyAnonymous_CanSelectButCannotInsert()
    {
        var config = new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            EnableAuthorization = true,
            AllowAnonymousSession = true,
            AnonymousRole = DatabaseRole.ReadOnly,
            AuthorizationUsers =
            [
                new DataVoAuthUser { Username = "admin", Password = "adminpw", Role = DatabaseRole.Admin }
            ]
        };

        using var context = new DataVoContext(config);
        string dbName = $"AuthAnon_{Guid.NewGuid():N}";

        Assert.True(context.Login("admin", "adminpw"));
        Assert.False(context.Login("admin", "wrong-password"));

        QueryResult createDb = context.Execute($"CREATE DATABASE {dbName}").Single();
        Assert.False(createDb.IsError, string.Join(" | ", createDb.Messages));

        QueryResult useDb = context.Execute($"USE {dbName}").Single();
        Assert.False(useDb.IsError, string.Join(" | ", useDb.Messages));

        QueryResult createTable = context.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)").Single();
        Assert.False(createTable.IsError, string.Join(" | ", createTable.Messages));

        QueryResult insertAsAdmin = context.Execute("INSERT INTO Users VALUES (1, 'Alice')").Single();
        Assert.False(insertAsAdmin.IsError, string.Join(" | ", insertAsAdmin.Messages));

        context.Logout();

        QueryResult selectAsAnonymous = context.Execute("SELECT * FROM Users").Single();
        Assert.False(selectAsAnonymous.IsError, string.Join(" | ", selectAsAnonymous.Messages));
        Assert.Single(selectAsAnonymous.Data);

        QueryResult insertAsAnonymous = context.Execute("INSERT INTO Users VALUES (2, 'Bob')").Single();
        Assert.True(insertAsAnonymous.IsError);
        Assert.Contains("not authorized", string.Join(" | ", insertAsAnonymous.Messages), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Authorization_ReadWrite_UserCannotManageSchema_ButCanWriteData()
    {
        var config = new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            EnableAuthorization = true,
            AllowAnonymousSession = false,
            AuthorizationUsers =
            [
                new DataVoAuthUser { Username = "admin", Password = "adminpw", Role = DatabaseRole.Admin },
                new DataVoAuthUser { Username = "writer", Password = "writerpw", Role = DatabaseRole.ReadWrite }
            ]
        };

        using var context = new DataVoContext(config);
        string dbName = $"AuthWriter_{Guid.NewGuid():N}";

        Assert.True(context.Login("admin", "adminpw"));
        QueryResult createDb = context.Execute($"CREATE DATABASE {dbName}").Single();
        Assert.False(createDb.IsError, string.Join(" | ", createDb.Messages));

        QueryResult useDb = context.Execute($"USE {dbName}").Single();
        Assert.False(useDb.IsError, string.Join(" | ", useDb.Messages));

        QueryResult createTable = context.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR)").Single();
        Assert.False(createTable.IsError, string.Join(" | ", createTable.Messages));

        context.Logout();
        Assert.True(context.Login("writer", "writerpw"));

        QueryResult createTableAsWriter = context.Execute("CREATE TABLE ShouldFail (Id INT)").Single();
        Assert.True(createTableAsWriter.IsError);
        Assert.Contains("not authorized", string.Join(" | ", createTableAsWriter.Messages), StringComparison.OrdinalIgnoreCase);

        QueryResult insertAsWriter = context.Execute("INSERT INTO Users VALUES (1, 'WriterUser')").Single();
        Assert.False(insertAsWriter.IsError, string.Join(" | ", insertAsWriter.Messages));

        QueryResult select = context.Execute("SELECT * FROM Users").Single();
        Assert.False(select.IsError, string.Join(" | ", select.Messages));
        Assert.Single(select.Data);
    }

    [Fact]
    public void Authorization_SqlUserRoleGrantRevoke_WithHashedCredentials_WorksEndToEnd()
    {
        var config = new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            EnableAuthorization = true,
            AllowAnonymousSession = false,
            AuthorizationUsers =
            [
                new DataVoAuthUser { Username = "admin", Password = "adminpw", Role = DatabaseRole.Admin }
            ]
        };

        using var context = new DataVoContext(config);
        string dbName = $"AuthSql_{Guid.NewGuid():N}";

        Assert.True(context.Login("admin", "adminpw"));

        QueryResult createDb = context.Execute($"CREATE DATABASE {dbName}").Single();
        Assert.False(createDb.IsError, string.Join(" | ", createDb.Messages));

        QueryResult useDb = context.Execute($"USE {dbName}").Single();
        Assert.False(useDb.IsError, string.Join(" | ", useDb.Messages));

        QueryResult createTable = context.Execute("CREATE TABLE Docs (Id INT PRIMARY KEY, Name VARCHAR)").Single();
        Assert.False(createTable.IsError, string.Join(" | ", createTable.Messages));

        QueryResult seed = context.Execute("INSERT INTO Docs VALUES (1, 'Seed')").Single();
        Assert.False(seed.IsError, string.Join(" | ", seed.Messages));

        Assert.False(context.Execute("CREATE ROLE analyst").Single().IsError);
        Assert.False(context.Execute("GRANT READ TO ROLE analyst").Single().IsError);
        Assert.False(context.Execute("CREATE USER alice IDENTIFIED BY 'alicepw' ROLE analyst").Single().IsError);

        QueryResult logout = context.Execute("LOGOUT").Single();
        Assert.False(logout.IsError, string.Join(" | ", logout.Messages));

        QueryResult unauthenticatedSelect = context.Execute("SELECT * FROM Docs").Single();
        Assert.True(unauthenticatedSelect.IsError);
        Assert.Contains("authentication is required", string.Join(" | ", unauthenticatedSelect.Messages), StringComparison.OrdinalIgnoreCase);

        QueryResult loginAlice = context.Execute("LOGIN alice IDENTIFIED BY 'alicepw'").Single();
        Assert.False(loginAlice.IsError, string.Join(" | ", loginAlice.Messages));

        QueryResult aliceSelect = context.Execute("SELECT * FROM Docs").Single();
        Assert.False(aliceSelect.IsError, string.Join(" | ", aliceSelect.Messages));
        Assert.Single(aliceSelect.Data);

        QueryResult aliceInsertDenied = context.Execute("INSERT INTO Docs VALUES (2, 'Denied')").Single();
        Assert.True(aliceInsertDenied.IsError);
        Assert.Contains("not authorized", string.Join(" | ", aliceInsertDenied.Messages), StringComparison.OrdinalIgnoreCase);

        Assert.False(context.Execute("LOGIN admin IDENTIFIED BY 'adminpw'").Single().IsError);
        Assert.False(context.Execute("GRANT WRITE TO alice").Single().IsError);

        Assert.False(context.Execute("LOGIN alice IDENTIFIED BY 'alicepw'").Single().IsError);
        QueryResult aliceInsertAllowed = context.Execute("INSERT INTO Docs VALUES (2, 'Allowed')").Single();
        Assert.False(aliceInsertAllowed.IsError, string.Join(" | ", aliceInsertAllowed.Messages));

        Assert.False(context.Execute("LOGIN admin IDENTIFIED BY 'adminpw'").Single().IsError);
        Assert.False(context.Execute("REVOKE WRITE FROM alice").Single().IsError);

        Assert.False(context.Execute("LOGIN alice IDENTIFIED BY 'alicepw'").Single().IsError);
        QueryResult aliceInsertRevoked = context.Execute("INSERT INTO Docs VALUES (3, 'Blocked')").Single();
        Assert.True(aliceInsertRevoked.IsError);
        Assert.Contains("not authorized", string.Join(" | ", aliceInsertRevoked.Messages), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Authorization_ShowSecurityIntrospectionCommands_ReturnExpectedRows()
    {
        var config = new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            EnableAuthorization = true,
            AllowAnonymousSession = false,
            AuthorizationUsers =
            [
                new DataVoAuthUser { Username = "admin", Password = "adminpw", Role = DatabaseRole.Admin }
            ]
        };

        using var context = new DataVoContext(config);
        Assert.True(context.Login("admin", "adminpw"));

        Assert.False(context.Execute("CREATE ROLE analyst").Single().IsError);
        Assert.False(context.Execute("GRANT READ TO ROLE analyst").Single().IsError);
        Assert.False(context.Execute("CREATE USER alice IDENTIFIED BY 'alicepw' ROLE analyst").Single().IsError);
        Assert.False(context.Execute("GRANT WRITE TO alice").Single().IsError);

        QueryResult showUsers = context.Execute("SHOW USERS").Single();
        Assert.False(showUsers.IsError, string.Join(" | ", showUsers.Messages));
        Dictionary<string, object?>? aliceUser = showUsers.Data.FirstOrDefault(row =>
            string.Equals(row["Username"]?.ToString(), "alice", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(aliceUser);
        Assert.Contains("analyst", aliceUser["Roles"]?.ToString() ?? string.Empty, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WriteData", aliceUser["DirectPermissions"]?.ToString() ?? string.Empty, StringComparison.OrdinalIgnoreCase);

        QueryResult showRoles = context.Execute("SHOW ROLES").Single();
        Assert.False(showRoles.IsError, string.Join(" | ", showRoles.Messages));
        Dictionary<string, object?>? analystRole = showRoles.Data.FirstOrDefault(row =>
            string.Equals(row["Role"]?.ToString(), "analyst", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(analystRole);
        Assert.Contains("ReadData", analystRole["Permissions"]?.ToString() ?? string.Empty, StringComparison.OrdinalIgnoreCase);

        QueryResult showGrants = context.Execute("SHOW GRANTS").Single();
        Assert.False(showGrants.IsError, string.Join(" | ", showGrants.Messages));
        Assert.Contains(showGrants.Data, row =>
            string.Equals(row["GranteeType"]?.ToString(), "USER", StringComparison.OrdinalIgnoreCase)
            && string.Equals(row["Grantee"]?.ToString(), "alice", StringComparison.OrdinalIgnoreCase)
            && string.Equals(row["GrantType"]?.ToString(), "ROLE", StringComparison.OrdinalIgnoreCase)
            && string.Equals(row["GrantValue"]?.ToString(), "analyst", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(showGrants.Data, row =>
            string.Equals(row["GranteeType"]?.ToString(), "ROLE", StringComparison.OrdinalIgnoreCase)
            && string.Equals(row["Grantee"]?.ToString(), "analyst", StringComparison.OrdinalIgnoreCase)
            && string.Equals(row["GrantType"]?.ToString(), "PERMISSION", StringComparison.OrdinalIgnoreCase)
            && string.Equals(row["GrantValue"]?.ToString(), "ReadData", StringComparison.OrdinalIgnoreCase));

        QueryResult showGrantsForUser = context.Execute("SHOW GRANTS FOR USER alice").Single();
        Assert.False(showGrantsForUser.IsError, string.Join(" | ", showGrantsForUser.Messages));
        Assert.NotEmpty(showGrantsForUser.Data);
        Assert.All(showGrantsForUser.Data, row =>
            Assert.Equal("USER", row["GranteeType"]?.ToString(), ignoreCase: true));
        Assert.All(showGrantsForUser.Data, row =>
            Assert.Equal("alice", row["Grantee"]?.ToString(), ignoreCase: true));

        QueryResult showGrantsForRole = context.Execute("SHOW GRANTS FOR ROLE analyst").Single();
        Assert.False(showGrantsForRole.IsError, string.Join(" | ", showGrantsForRole.Messages));
        Assert.NotEmpty(showGrantsForRole.Data);
        Assert.All(showGrantsForRole.Data, row =>
            Assert.Equal("ROLE", row["GranteeType"]?.ToString(), ignoreCase: true));
        Assert.All(showGrantsForRole.Data, row =>
            Assert.Equal("analyst", row["Grantee"]?.ToString(), ignoreCase: true));
    }
}
