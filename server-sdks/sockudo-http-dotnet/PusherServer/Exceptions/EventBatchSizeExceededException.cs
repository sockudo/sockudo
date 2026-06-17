using System;

namespace PusherServer.Exceptions
{
    /// <summary>
    /// Thrown when the number of events triggered in a batch exceeds the limit allowed.
    /// </summary>
    public class EventBatchSizeExceededException : ArgumentOutOfRangeException
    {
        /// <summary>
        /// Creates an instance of a <see cref="EventBatchSizeExceededException" />.
        /// </summary>
        /// <param name="paramName">The name of the parameter that causes this exception.</param>
        /// <param name="actualValue">The batch size that causes this exception.</param>
        public EventBatchSizeExceededException(string paramName, int actualValue) :
            base(paramName, actualValue, $"Only {ValidationHelper.MAX_BATCH_SIZE} events permitted per batch.")
        {
        }
    }
}