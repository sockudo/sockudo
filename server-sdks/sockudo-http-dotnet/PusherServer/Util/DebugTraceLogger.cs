using System.Diagnostics;

namespace PusherServer.Util
{
    /// <summary>
    /// Traces debug messages using <see cref="System.Diagnostics.Debug"/>.
    /// </summary>
    public class DebugTraceLogger : ITraceLogger
    {
        /// <inheritdoc/>
        public void Trace(string message)
        {
            Debug.WriteLine(message);
        }
    }
}
