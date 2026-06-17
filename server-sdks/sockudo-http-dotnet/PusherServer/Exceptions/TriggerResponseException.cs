using System;

namespace PusherServer.Exceptions
{
    /// <summary>
    /// Thrown when problems are detected with the response from the Pusher trigger HTTP endpoint.
    /// </summary>
    public class TriggerResponseException : Exception
    {
        /// <summary>
        /// Create a new instance
        /// </summary>
        /// <param name="message">Description of the exception</param>
        /// <param name="innerException">The inner exception</param>
        public TriggerResponseException(string message, Exception innerException) :
            base(message, innerException)
        {
        }
    }
}