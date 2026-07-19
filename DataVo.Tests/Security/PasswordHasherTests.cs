using System.Security.Cryptography;
using DataVo.Core.Runtime.Security;

namespace DataVo.Tests.Security;

public sealed class PasswordHasherTests
{
    private const string PersistedPassword = "DataVo-stage2b!";
    private const string PersistedHash = "rb/gy2xJgDHSNv4ThGEcdnh+Dlu8dEVmWN+qVF3It4w=";
    private const string PersistedSalt = "AAECAwQFBgcICQoLDA0ODw==";

    [Fact]
    public void Verify_AcceptsPersistedPbkdf2Sha256Vector()
    {
        Assert.True(PasswordHasher.Verify(PersistedPassword, PersistedHash, PersistedSalt));
        Assert.False(PasswordHasher.Verify("wrong-password", PersistedHash, PersistedSalt));
    }

    [Fact]
    public void PasswordHashPlatform_DerivationMatchesPersistedVector()
    {
        byte[] salt = Convert.FromBase64String(PersistedSalt);

        byte[] hash = PasswordHashPlatform.DeriveHash(
            PersistedPassword,
            salt,
            iterations: 210_000,
            HashAlgorithmName.SHA256,
            hashSizeBytes: 32);

        Assert.Equal(PersistedHash, Convert.ToBase64String(hash));
    }

    [Fact]
    public void HashPassword_GeneratesExpectedSizesAndVerifiesOnlyCorrectPassword()
    {
        (string hash, string salt) = PasswordHasher.HashPassword("correct horse battery staple");

        Assert.Equal(32, Convert.FromBase64String(hash).Length);
        Assert.Equal(16, Convert.FromBase64String(salt).Length);
        Assert.True(PasswordHasher.Verify("correct horse battery staple", hash, salt));
        Assert.False(PasswordHasher.Verify("incorrect", hash, salt));
    }

    [Fact]
    public void HashPassword_UsesDistinctRandomSaltForEachHash()
    {
        (string firstHash, string firstSalt) = PasswordHasher.HashPassword("same-password");
        (string secondHash, string secondSalt) = PasswordHasher.HashPassword("same-password");

        Assert.NotEqual(firstSalt, secondSalt);
        Assert.NotEqual(firstHash, secondHash);
    }

    [Theory]
    [InlineData("", "AAECAwQFBgcICQoLDA0ODw==")]
    [InlineData("   ", "AAECAwQFBgcICQoLDA0ODw==")]
    [InlineData("not-base64", "AAECAwQFBgcICQoLDA0ODw==")]
    [InlineData("rb/gy2xJgDHSNv4ThGEcdnh+Dlu8dEVmWN+qVF3It4w=", "")]
    [InlineData("rb/gy2xJgDHSNv4ThGEcdnh+Dlu8dEVmWN+qVF3It4w=", "   ")]
    [InlineData("rb/gy2xJgDHSNv4ThGEcdnh+Dlu8dEVmWN+qVF3It4w=", "not-base64")]
    public void Verify_ReturnsFalseForMalformedPersistedValues(string hash, string salt)
    {
        Assert.False(PasswordHasher.Verify(PersistedPassword, hash, salt));
    }
}
