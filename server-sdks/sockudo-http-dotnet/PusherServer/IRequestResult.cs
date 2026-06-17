using System.Net;

namespace PusherServer
{
    /// <summary>
    /// Base interface for all Request Results
    /// </summary>
    public interface IRequestResult
    {
        /// <summary>
        /// Gets the Body from a Request Result
        /// </summary>
        string Body { get; }

        /// <summary>
        /// Gets the Status Code from a Request Result
        /// </summary>
        HttpStatusCode StatusCode { get; }
    }
}
