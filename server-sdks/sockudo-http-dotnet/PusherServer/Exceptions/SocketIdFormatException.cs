using System;

namespace PusherServer.Exceptions
{
    /// <summary>
    /// Thrown when an invalid socket id is used when triggering an event.
    /// </summary>
    public class SocketIdFormatException : FormatException
    {
        /// <summary>
        /// Creates an instance of a <see cref="SocketIdFormatException" />.
        /// </summary>
        /// <param name="actualValue">The invalid socket id that causes this exception.</param>
        public SocketIdFormatException(string actualValue) :
            base($"The socket id \"{actualValue}\" was not in the form: {ValidationHelper.SOCKET_ID_REGEX}")
        {
        }
    }
}