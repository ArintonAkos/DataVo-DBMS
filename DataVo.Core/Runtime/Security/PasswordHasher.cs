using System.Text;

namespace DataVo.Core.Runtime.Security;

/// <summary>
/// Provides deterministic, browser-safe password hashing and verification helpers.
/// </summary>
internal static class PasswordHasher
{
    private const int SaltSizeBytes = 16;
    private const int HashSizeBytes = 32;
    private const int IterationCount = 10_000;
    private static long _saltCounter;

    /// <summary>
    /// Hashes a plaintext password and returns base64-encoded hash and salt values.
    /// </summary>
    /// <param name="password">The plaintext password to hash.</param>
    /// <returns>A tuple containing the derived hash and salt as base64 strings.</returns>
    public static (string Hash, string Salt) HashPassword(string password)
    {
        byte[] salt = new byte[SaltSizeBytes];
        FillSalt(salt);
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
        catch
        {
            return false;
        }

        byte[] computedHash = DeriveHash(password, salt, expectedHash.Length);

        if (computedHash.Length != expectedHash.Length)
        {
            return false;
        }

        int diff = 0;
        for (int i = 0; i < computedHash.Length; i++)
        {
            diff |= computedHash[i] ^ expectedHash[i];
        }

        return diff == 0;
    }

    /// <summary>
    /// Derives a fixed-size hash buffer from password and salt using iterative state mixing.
    /// </summary>
    /// <param name="password">The plaintext password input.</param>
    /// <param name="salt">The raw salt bytes.</param>
    /// <param name="outputLength">The requested hash length in bytes.</param>
    /// <returns>A derived hash byte array with <paramref name="outputLength"/> bytes.</returns>
    private static byte[] DeriveHash(string password, byte[] salt, int outputLength)
    {
        byte[] passwordBytes = Encoding.UTF8.GetBytes(password ?? string.Empty);
        byte[] output = new byte[outputLength];

        ulong state = 1469598103934665603UL;
        for (int iteration = 0; iteration < IterationCount; iteration++)
        {
            state ^= (ulong)(iteration & 0xFF);
            state *= 1099511628211UL;

            for (int i = 0; i < salt.Length; i++)
            {
                state ^= salt[i];
                state *= 1099511628211UL;
            }

            for (int i = 0; i < passwordBytes.Length; i++)
            {
                state ^= passwordBytes[i];
                state *= 1099511628211UL;
            }

            for (int i = 0; i < outputLength; i++)
            {
                output[i] ^= (byte)((state >> ((i % 8) * 8)) & 0xFF);
                state = RotateLeft(state, 5) ^ (ulong)(i + 1);
            }
        }

        return output;
    }

    /// <summary>
    /// Performs a 64-bit rotate-left operation.
    /// </summary>
    /// <param name="value">The value to rotate.</param>
    /// <param name="bits">The number of bits to rotate left.</param>
    /// <returns>The rotated 64-bit value.</returns>
    private static ulong RotateLeft(ulong value, int bits)
    {
        return (value << bits) | (value >> (64 - bits));
    }

    /// <summary>
    /// Fills the provided buffer with non-cryptographic salt bytes suitable for browser runtime compatibility.
    /// </summary>
    /// <param name="buffer">The destination salt buffer to fill.</param>
    private static void FillSalt(byte[] buffer)
    {
        ulong state = (ulong)DateTime.UtcNow.Ticks
            ^ (ulong)Environment.TickCount64
            ^ (ulong)Interlocked.Increment(ref _saltCounter);

        for (int i = 0; i < buffer.Length; i++)
        {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            buffer[i] = (byte)(state & 0xFF);
        }
    }
}
