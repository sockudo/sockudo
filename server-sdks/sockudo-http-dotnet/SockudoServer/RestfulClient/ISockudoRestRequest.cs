using System.Collections.Generic;

namespace SockudoServer.RestfulClient
{
    /// <summary>
    /// The contract for a REST request to be made to the Sockudo API
    /// </summary>
    public interface ISockudoRestRequest
    {
        /// <summary>
        /// Gets or sets the type of RESTful call to make
        /// </summary>
        SockudoMethod Method { get; set; }

        /// <summary>
        /// Gets the Resource Uri for this request
        /// </summary>
        string ResourceUri { get; }

        /// <summary>
        /// Gets or sets the content that will be sent with the request
        /// </summary>
        object Body { get; set; }

        /// <summary>
        /// Gets the additional HTTP headers to include with the request.
        /// </summary>
        IDictionary<string, string> Headers { get; }

        /// <summary>
        /// Gets the current body as a Json String
        /// </summary>
        /// <returns></returns>
        string GetContentAsJsonString();
    }
}
