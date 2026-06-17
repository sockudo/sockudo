
using System.Collections.Generic;

namespace SockudoServer
{
    /// <summary>
    /// Represents the Options that can be used by A Trigger
    /// </summary>
    public class TriggerOptions : ITriggerOptions
    {
        /// <summary>
        /// Gets or sets the Socket ID for the consuming Trigger
        /// </summary>
        public string SocketId { get; set; }

        /// <summary>
        /// List of attributes that should be returned for each unique channel triggered to.
        /// </summary>
        public List<string> Info { get; set; }

        /// <summary>
        /// An optional idempotency key for deduplicating the trigger request.
        /// When provided, it is included in the JSON body and sent as an X-Idempotency-Key header.
        /// </summary>
        public string IdempotencyKey { get; set; }

        /// <summary>
        /// Optional V2 extras for the event.
        /// </summary>
        public MessageExtras Extras { get; set; }
    }
}
