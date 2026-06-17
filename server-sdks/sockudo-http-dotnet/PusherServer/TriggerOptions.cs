
using System.Collections.Generic;

namespace PusherServer
{
    /// <summary>
    /// Represents the Options that can be used by A Trigger
    /// </summary>
    public class TriggerOptions: ITriggerOptions
    {
        /// <summary>
        /// Gets or sets the Socket ID for the consuming Trigger
        /// </summary>
        public string SocketId { get; set; }

        /// <summary>
        /// List of attributes that should be returned for each unique channel triggered to.
        /// </summary>
        public List<string> Info { get; set; }
    }
}
