using System.Collections.Generic;

namespace SockudoServer
{
    /// <summary>
    /// V2 extras for publish events.
    /// </summary>
    public class MessageExtras
    {
        /// <summary>
        /// Flat key-value headers.
        /// </summary>
        public Dictionary<string, object> Headers { get; set; }

        /// <summary>
        /// Whether the event is ephemeral (not stored in history).
        /// </summary>
        public bool? Ephemeral { get; set; }

        /// <summary>
        /// An idempotency key for deduplication within extras.
        /// </summary>
        public string IdempotencyKey { get; set; }

        /// <summary>
        /// Whether the event should be echoed back to the sender.
        /// </summary>
        public bool? Echo { get; set; }
    }

    /// <summary>
    /// Represents an event for batch submission
    /// </summary>
    public class Event
    {
        /// <summary>
        /// The event name
        /// </summary>
        public string EventName { get; set; }

        /// <summary>
        /// The channel to which the event should be sent
        /// </summary>
        public string Channel { get; set; }

        /// <summary>
        /// An optional socket ID which should not receive the event
        /// </summary>
        public string SocketId { get; set; }

        /// <summary>
        /// The event data
        /// </summary>
        public object Data { get; set; }

        /// <summary>
        /// An optional idempotency key for deduplicating this event.
        /// When provided, it is included in the JSON body and sent as an X-Idempotency-Key header.
        /// </summary>
        public string IdempotencyKey { get; set; }

        /// <summary>
        /// Optional V2 extras for the event.
        /// </summary>
        public MessageExtras Extras { get; set; }
    }
}
