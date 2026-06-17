
using System.Collections.Generic;

namespace PusherServer
{
    /// <summary>
    /// Class used for handling the deserialisation of the Trigger HTTP response.
    /// </summary>
    public class EventIdData
    {
        private readonly Dictionary<string, string> _eventIds = new Dictionary<string, string>();
        private readonly Dictionary<string, ChannelAttributes> _channelStates = new Dictionary<string, ChannelAttributes>();

        /// <summary>
        /// Dictionary of channel name to event ID for the triggered event.
        /// </summary>
        public Dictionary<string, string> event_ids
        {
            get
            {
                return _eventIds;
            }
        }

        /// <summary>
        /// Dictionary of channel name to channel attributes
        /// </summary>
        public Dictionary<string, ChannelAttributes> channels => _channelStates;
    }
}