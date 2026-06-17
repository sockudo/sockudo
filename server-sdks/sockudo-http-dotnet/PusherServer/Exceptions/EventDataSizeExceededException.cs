using System;

namespace PusherServer.Exceptions
{
    /// <summary>
    /// Thrown when the size of the <c>Data</c> for an <see cref="Event"/> exceeds the byte size limit.
    /// </summary>
    public class EventDataSizeExceededException : InvalidOperationException
    {
        /// <summary>
        /// Creates an instance of a <see cref="EventDataSizeExceededException" />.
        /// </summary>
        /// <param name="sizeLimitInBytes">The maximum size in bytes allowed for event data.</param>
        /// <param name="actualValue">The invalid event data size in bytes that causes this exception.</param>
        public EventDataSizeExceededException(int sizeLimitInBytes, int actualValue) :
            base($"The data content of this event exceeds the allowed maximum ({sizeLimitInBytes} bytes). The actual size is {actualValue} bytes.")
        {
        }

        /// <summary>
        /// Gets or sets the name of a channel associated with event data that has caused the exception.
        /// </summary>
        public string ChannelName { get; set; }

        /// <summary>
        /// Gets or sets the name of an event associated with data that has caused the exception.
        /// </summary>
        public string EventName { get; set; }
    }
}