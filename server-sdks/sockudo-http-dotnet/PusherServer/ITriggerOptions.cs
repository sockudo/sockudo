using System.Collections.Generic;

namespace PusherServer
{
    /// <summary>
    /// Additional options that can be used when triggering an event.
    /// </summary>
    public interface ITriggerOptions
    {
        /// <summary>
        /// Gets or sets the Socket ID for a consuming Trigger
        /// </summary>
        string SocketId { get; set; }

        /// <summary>
        /// List of attributes that should be returned for each unique channel triggered to.
        /// </summary>
        List<string> Info { get; set; }
    }
}
