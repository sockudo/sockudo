using System;

namespace PusherServer.Exceptions
{
    /// <summary>
    /// Thrown when a channel name is too long.
    /// </summary>
    public class ChannelNameLengthExceededException : ArgumentOutOfRangeException
    {
        /// <summary>
        /// Creates an instance of a <see cref="ChannelNameLengthExceededException" />.
        /// </summary>
        /// <param name="paramName">The name of the parameter that causes this exception.</param>
        /// <param name="actualValue">The length of the channel name that causes this exception.</param>
        public ChannelNameLengthExceededException(string paramName, int actualValue) :
            base(paramName, actualValue, $"The length of the channel name is greater than the allowed {ValidationHelper.CHANNEL_NAME_MAX_LENGTH} characters.")
        {
        }
    }
}