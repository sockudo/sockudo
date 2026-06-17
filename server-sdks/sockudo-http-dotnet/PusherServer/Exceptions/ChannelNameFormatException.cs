using System;

namespace PusherServer.Exceptions
{
    /// <summary>
    /// Thrown when an invalid channel name is used when triggering an event.
    /// </summary>
    public class ChannelNameFormatException : FormatException
    {
        /// <summary>
        /// Creates an instance of a <see cref="ChannelNameFormatException" />.
        /// </summary>
        /// <param name="actualValue">The invalid channel name that causes this exception.</param>
        public ChannelNameFormatException(string actualValue) :
            base($"The channel name \"{actualValue}\" was not in the form: {ValidationHelper.CHANNEL_NAME_REGEX}")
        {
        }
    }
}