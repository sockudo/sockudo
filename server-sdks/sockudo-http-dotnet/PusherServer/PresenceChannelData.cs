
namespace PusherServer
{
    /// <summary>
    /// Information about a user who is subscribing to a presence channel.
    /// </summary>
    public class PresenceChannelData
    {
        /// <summary>
        /// A unique user identifier for the user witin the application.
        /// </summary>
        /// <remarks>
        /// Pusher uses this to uniquely identify a user. So, if multiple users are given the same <code>user_id</code>
        /// the second of these users will be ignored and won't be represented on the presence channel.
        /// </remarks>
        public string user_id { get; set; }

        /// <summary>
        /// Arbitrary additional information about the user.
        /// </summary>
        public object user_info { get; set; }
    }
}
