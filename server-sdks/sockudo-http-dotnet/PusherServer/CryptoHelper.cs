using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace PusherServer
{
    internal class CryptoHelper
    {
        internal static string GetMd5Hash(string jsonData)
        {
            using (var md5 = MD5.Create())
            {
                var result = md5.ComputeHash(Encoding.UTF8.GetBytes(jsonData));
                return BytesToHex(result);
            }
        }

        private static string BytesToHex(IEnumerable<byte> byteArray)
        {
            return String.Concat(byteArray.Select(bytes => bytes.ToString("x2")).ToArray());
        }

        internal static string GetHmac256(string secret, string toSign)
        {
            using (var hmacsha256 = new HMACSHA256(Encoding.UTF8.GetBytes(secret)))
            {
                var hash = hmacsha256.ComputeHash(Encoding.UTF8.GetBytes(toSign));
                return BytesToHex(hash);
            }
        }

        internal static byte[] GenerateSharedSecretHash(byte[] key, string toSign)
        {
            using (var hmacsha256 = new HMACSHA256(key))
            {
                return hmacsha256.ComputeHash(Encoding.UTF8.GetBytes(toSign));
            }
        }

        internal static string GenerateSharedSecret(byte[] key, string toSign)
        {
            return Convert.ToBase64String(GenerateSharedSecretHash(key, toSign));
        }
    }
}
