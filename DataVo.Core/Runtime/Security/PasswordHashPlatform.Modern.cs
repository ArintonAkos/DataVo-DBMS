#if NET10_0_OR_GREATER
using System.Security.Cryptography;

namespace DataVo.Core.Runtime.Security;

internal static class PasswordHashPlatform
{
    public static byte[] GetRandomBytes(int count) => RandomNumberGenerator.GetBytes(count);

    public static byte[] DeriveHash(
        string password,
        byte[] salt,
        int iterations,
        HashAlgorithmName algorithm,
        int hashSizeBytes) =>
        Rfc2898DeriveBytes.Pbkdf2(password, salt, iterations, algorithm, hashSizeBytes);
}
#endif
