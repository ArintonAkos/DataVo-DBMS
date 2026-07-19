#if NETSTANDARD2_1
using System.Security.Cryptography;

namespace DataVo.Core.Runtime.Security;

internal static class PasswordHashPlatform
{
    public static byte[] GetRandomBytes(int count)
    {
        var bytes = new byte[count];
        using RandomNumberGenerator random = RandomNumberGenerator.Create();
        random.GetBytes(bytes);
        return bytes;
    }

    public static byte[] DeriveHash(
        string password,
        byte[] salt,
        int iterations,
        HashAlgorithmName algorithm,
        int hashSizeBytes)
    {
        using var derivation = new Rfc2898DeriveBytes(password, salt, iterations, algorithm);
        return derivation.GetBytes(hashSizeBytes);
    }
}
#endif
