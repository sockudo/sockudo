using System;

namespace PusherServer.Exceptions
{
    /// <summary>
    /// Thrown when an encryption master key is invalid.
    /// </summary>
    public class EncryptionMasterKeyException : ArgumentOutOfRangeException
    {
        /// <summary>
        /// Creates an instance of an <see cref="EncryptionMasterKeyException" />.
        /// </summary>
        /// <param name="paramName">The name of the parameter that causes this exception.</param>
        /// <param name="actualValue">The length of the key that causes this exception.</param>
        public EncryptionMasterKeyException(string paramName, int actualValue) 
            : base(paramName, actualValue, $"The encryption master key is expected to be {ValidationHelper.ENCRYPTION_MASTER_KEY_LENGTH} bytes in length.")
        {
        }
    }
}