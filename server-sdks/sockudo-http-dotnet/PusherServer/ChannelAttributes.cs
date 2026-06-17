namespace PusherServer
{
    /// <summary>
    /// Channel attributes as returned in the trigger event endpoint
    /// </summary>
    public class ChannelAttributes
    {
        /// <summary>
        /// Number of distinct users currently subscribed to each channel (a single user may be subscribed many times, but will only count as one)
        /// </summary>
        public int user_count { get; set; }
        
        /// <summary>
        /// Number of connections currently subscribed to each channel. This attribute is not available by default. To enable it, navigate to your Channels dashboard, find the app you are working on, and click App Settings.
        /// </summary>
        public int subscription_count { get; set; }
    }
}