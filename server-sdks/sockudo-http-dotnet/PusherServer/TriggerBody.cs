using System.Collections.Generic;

namespace PusherServer
{
    /// <summary>
    /// Represents the payload to be sent when triggering events
    /// </summary>
    internal class TriggerBody
    {
        /// <summary>
        /// The name of the event
        /// </summary>
        public string name { get; set; }

        /// <summary>
        /// The event data
        /// </summary>
        public string data { get; set; }

        /// <summary>
        /// The channels the event should be triggered on.
        /// </summary>
        public string[] channels { get; set; }

        /// <summary>
        /// The id of a socket to be excluded from receiving the event.
        /// </summary>
        public string socket_id { get; set; }

        /// <summary>
        /// A comma-separated list of attributes that should be returned for each unique channel triggered to.
        /// </summary>
        public string info { get; set; }
    }
}
