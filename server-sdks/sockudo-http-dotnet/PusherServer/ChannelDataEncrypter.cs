using NaCl;
using System.Security.Cryptography;
using System.Text;

namespace PusherServer
{
    internal class ChannelDataEncrypter : IChannelDataEncrypter
    {
        EncryptedChannelData IChannelDataEncrypter.EncryptData(string channelName, string jsonData, byte[] encryptionMasterKey)
        {
            ValidationHelper.ValidateChannelName(channelName);
            ValidationHelper.ValidateEncryptionMasterKey(encryptionMasterKey);

            EncryptedChannelData result = null;
            if (jsonData != null)
            {
                byte[] sharedSecret = CryptoHelper.GenerateSharedSecretHash(encryptionMasterKey, channelName);
                using (XSalsa20Poly1305 secretBox = new XSalsa20Poly1305(sharedSecret))
                using (RandomNumberGenerator random = RandomNumberGenerator.Create())
                {
                    byte[] message = Encoding.UTF8.GetBytes(jsonData);
                    byte[] cipher = new byte[message.Length + XSalsa20Poly1305.TagLength];
                    byte[] nonce = new byte[XSalsa20Poly1305.NonceLength];
                    random.GetBytes(nonce);
                    secretBox.Encrypt(cipher, message, nonce);
                    result = new EncryptedChannelData(nonceBytes: nonce, encrypted: cipher);
                }
            }

            return result;
        }
    }
}
