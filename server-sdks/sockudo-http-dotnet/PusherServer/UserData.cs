using System.Collections.Generic;

namespace PusherServer
{
    /// <summary>
    /// Information about a user who is authenticating to Pusher.
    /// </summary>
    public class UserData
    {
        /// <summary>
        /// A unique user identifier for the user witin the application.
        /// </summary>
        /// <remarks>
        /// Pusher uses this to uniquely identify a user.
        /// </remarks>
        public string id { get; set; }

        /// <summary>
        /// A list of user ids representing the circle of interest for this user.
        /// </summary>
        public string[] watchlist { get; set; }

        /// <summary>
        /// Arbitrary additional information about the user.
        /// </summary>
        public object user_info { get; set; }
    }
}
