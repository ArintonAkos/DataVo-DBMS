using System.Security.Cryptography;

namespace DataVo.Core.Runtime.Security;

/// <summary>
/// Provides cryptographically strong password hashing and verification helpers,
/// built on a cryptographic RNG salt and PBKDF2 (RFC 2898) key derivation.
/// </summary>
internal static class PasswordHasher
{
    private const int SaltSizeBytes = 16;
    private const int HashSizeBytes = 32;

    // PBKDF2-HMAC-SHA256 work factor. Aligned with the OWASP Password Storage
    // Cheat Sheet recommendation for PBKDF2-HMAC-SHA256.
    private const int IterationCount = 210_000;
    private static readonly HashAlgorithmName KdfAlgorithm = HashAlgorithmName.SHA256;

    /// <summary>
    /// Hashes a plaintext password and returns base64-encoded hash and salt values.
    /// </summary>
    /// <param name="password">The plaintext password to hash.</param>
    /// <returns>A tuple containing the derived hash and salt as base64 strings.</returns>
    public static (string Hash, string Salt) HashPassword(string password)
    {
#if NET6_0_OR_GREATER
        byte[] salt = RandomNumberGenerator.GetBytes(SaltSizeBytes);
#else
        byte[] salt = new byte[SaltSizeBytes];
        using (RandomNumberGenerator rng = RandomNumberGenerator.Create())
        {
            rng.GetBytes(salt);
        }
#endif

        byte[] hash = DeriveHash(password, salt, HashSizeBytes);

        return (Convert.ToBase64String(hash), Convert.ToBase64String(salt));
    }

    /// <summary>
    /// Verifies a plaintext password against persisted base64 hash and salt values.
    /// </summary>
    /// <param name="password">The plaintext password to verify.</param>
    /// <param name="hashBase64">The persisted base64-encoded hash value.</param>
    /// <param name="saltBase64">The persisted base64-encoded salt value.</param>
    /// <returns><see langword="true"/> when verification succeeds; otherwise <see langword="false"/>.</returns>
    public static bool Verify(string password, string hashBase64, string saltBase64)
    {
        if (string.IsNullOrWhiteSpace(hashBase64) || string.IsNullOrWhiteSpace(saltBase64))
        {
            return false;
        }

        byte[] expectedHash;
        byte[] salt;
        try
        {
            expectedHash = Convert.FromBase64String(hashBase64);
            salt = Convert.FromBase64String(saltBase64);
        }
        catch (FormatException)
        {
            return false;
        }

        if (expectedHash.Length == 0)
        {
            return false;
        }

        byte[] computedHash = DeriveHash(password, salt, expectedHash.Length);

        // Constant-time comparison to avoid leaking match progress via timing.
        return CryptographicOperations.FixedTimeEquals(computedHash, expectedHash);
    }

    private static byte[] DeriveHash(string? password, byte[] salt, int hashSizeBytes)
    {
#if NET6_0_OR_GREATER
        return Rfc2898DeriveBytes.Pbkdf2(
            password ?? string.Empty,
            salt,
            IterationCount,
            KdfAlgorithm,
            hashSizeBytes);
#else
        using var pbkdf2 = new Rfc2898DeriveBytes(
            password ?? string.Empty,
            salt,
            IterationCount,
            KdfAlgorithm);
        return pbkdf2.GetBytes(hashSizeBytes);
#endif
    }
}
