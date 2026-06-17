using System.Collections.Generic;

namespace SockudoServer
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

        /// <summary>
        /// An optional idempotency key for deduplicating the trigger request.
        /// When provided, it is included in the JSON body and sent as an X-Idempotency-Key header.
        /// </summary>
        string IdempotencyKey { get; set; }

        /// <summary>
        /// Optional V2 extras for the event.
        /// </summary>
        MessageExtras Extras { get; set; }
    }
}
