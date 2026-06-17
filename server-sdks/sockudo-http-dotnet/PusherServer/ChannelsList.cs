using System;
using System.Runtime.Serialization;
using System.Collections.Generic;

namespace PusherServer
{
    /*
    {"channels":{"test_channel":{}}}
    */

    /// <summary>
    /// A list of Channels received from the Pusher Server
    /// </summary>
    [DataContract]
    public class ChannelsList
    {
        private Dictionary<string, Dictionary<string, string>> _channelsInfo = null;

        /// <summary>
        /// Gets or sets the Channel Info for a given Channel Name
        /// </summary>
        /// <param name="channelName"></param>
        /// <returns>A string representing the Channel Info</returns>
        public Dictionary<string, string> this[string channelName]
        {
            get
            {
                return _channelsInfo[channelName];
            }
            set
            {
                _channelsInfo[channelName] = value;
            }
        }

        /// <summary>
        /// Gets or sets all the Channel Info
        /// </summary>
        [DataMember(Name = "channels")]
        public Dictionary<string, Dictionary<string, string>> Channels
        {
            get
            {
                return _channelsInfo;
            }
            set
            {
                if (_channelsInfo != null)
                {
                    throw new InvalidOperationException("Channels should only be set as part of deserialization.");
                }
                _channelsInfo = value;
            }
        }
    }
}
