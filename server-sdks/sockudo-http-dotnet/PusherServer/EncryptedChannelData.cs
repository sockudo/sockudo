using System;

namespace PusherServer
{
    internal class EncryptedChannelData
    {
        public EncryptedChannelData(byte[] nonceBytes, byte[] encrypted)
        {
            this.nonce = Convert.ToBase64String(nonceBytes);
            this.ciphertext = Convert.ToBase64String(encrypted);
        }

        public string nonce { get; set; }

        public string ciphertext { get; set; }
    }
}
