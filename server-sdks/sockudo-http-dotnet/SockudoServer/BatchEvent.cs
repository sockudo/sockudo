namespace SockudoServer
{
    /// <summary>
    /// Represents an event that is part of a batch trigger, for the purposes of serialisation
    /// </summary>
    internal class BatchEvent
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
        /// The channel the event should be triggered on.
        /// </summary>
        public string channel { get; set; }

        /// <summary>
        /// The id of a socket to be excluded from receiving the event.
        /// </summary>
        public string socket_id { get; set; }

        /// <summary>
        /// An optional idempotency key for deduplicating this event.
        /// </summary>
        public string idempotency_key { get; set; }

        /// <summary>
        /// Optional V2 extras for the event.
        /// </summary>
        public MessageExtras extras { get; set; }
    }
}
