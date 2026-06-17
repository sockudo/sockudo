using System;
using System.Collections.Generic;
using System.Net.Http;
using Newtonsoft.Json;
using PusherServer.Exceptions;
using PusherServer.Util;

namespace PusherServer
{
    /// <summary>
    /// The response from a Trigger REST request
    /// </summary>
    public class TriggerResult : RequestResult, ITriggerResult
    {
        /// <summary>
        /// Constructs a new instance of a TriggerResult based upon a passed in Rest Response
        /// </summary>
        /// <param name="response">The Rest Response which will form the basis of this Trigger Result</param>
        /// <param name="responseContent">The response content as a string</param>
        public TriggerResult(HttpResponseMessage response, string responseContent) : base(response, responseContent)
        {
            EventIdData eventIdData;

            try
            {
                eventIdData = JsonConvert.DeserializeObject<EventIdData>(responseContent);
            }
            catch (Exception e)
            {
                string msg = $"The response body from the Pusher HTTP endpoint could not be parsed as JSON: {Environment.NewLine}{responseContent}";
                throw new TriggerResponseException(msg, e);
            }

            EventIds = new ReadOnlyDictionary<string, string>(eventIdData.event_ids);
            ChannelAttributes = new ReadOnlyDictionary<string, ChannelAttributes>(eventIdData.channels);
        }

        /// <inheritDoc/>
        public IDictionary<string, string> EventIds { get; }
        public IDictionary<string, ChannelAttributes> ChannelAttributes { get; }
    }
}