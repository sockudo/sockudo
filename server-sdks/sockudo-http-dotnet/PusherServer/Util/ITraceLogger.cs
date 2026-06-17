namespace PusherServer.Util
{
    /// <summary>
    /// An interface for tracing debug messages.
    /// </summary>
    public interface ITraceLogger
    {
        /// <summary>
        /// Traces a debug message.
        /// </summary>
        /// <param name="message">The message to trace.</param>
        void Trace(string message);
    }
}
