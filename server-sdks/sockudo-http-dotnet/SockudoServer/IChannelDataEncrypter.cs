namespace SockudoServer
{
    internal interface IChannelDataEncrypter
    {
        EncryptedChannelData EncryptData(string channelName, string jsonData, byte[] encryptionMasterKey);
    }
}
