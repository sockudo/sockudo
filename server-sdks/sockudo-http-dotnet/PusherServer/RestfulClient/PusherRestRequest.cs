using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace PusherServer.RestfulClient
{
    /// <summary>
    /// A REST request to be made to the Pusher API
    /// </summary>
    public class PusherRestRequest : IPusherRestRequest
    {
        /// <summary>
        /// Creates a new REST request to make back to Pusher HQ
        /// </summary>
        /// <param name="resourceUri">The URI to call</param>
        public PusherRestRequest(string resourceUri)
        {
            if (string.IsNullOrWhiteSpace(resourceUri))
                throw new ArgumentNullException(nameof(resourceUri), "The resource URI must be a populated string");

            ResourceUri = resourceUri;
            Headers = new Dictionary<string, string>();
        }

        /// <inheritdoc/>
        public PusherMethod Method { get; set; }

        /// <inheritdoc/>
        public string ResourceUri { get; }

        /// <inheritdoc/>
        public object Body { get; set; }

        /// <inheritdoc/>
        public IDictionary<string, string> Headers { get; }

        /// <inheritdoc/>
        public string GetContentAsJsonString()
        {
            return Body != null ? DefaultSerializer.Default.Serialize(Body) : null;
        }
    }
}
