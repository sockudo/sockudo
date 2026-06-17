namespace PusherServer
{
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
    }
}
