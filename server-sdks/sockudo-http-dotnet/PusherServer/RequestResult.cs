using System;
using System.Net;
using System.Net.Http;

namespace PusherServer
{
    /// <summary>
    /// Abstract base class for results coming back from request to the Pusher servers
    /// </summary>
    public abstract class RequestResult : IRequestResult
    {
        /// <summary>
        /// Constructor to constract the abstract base class for classes derived from RequestResults
        /// </summary>
        /// <param name="response"></param>
        /// <param name="originalContent"></param>
        protected RequestResult(HttpResponseMessage response, string originalContent)
        {
            if (response == null)
            {
                throw new ArgumentNullException(nameof(response));
            }

            Body = originalContent;
            StatusCode = response.StatusCode;

            Response = response;
        }

        /// <summary>
        /// Gets the Status Code returned in the wrapped Rest Response
        /// </summary>
        public HttpStatusCode StatusCode { get; protected set; }

        /// <summary>
        /// Gets the Body returned in the wrapped Rest Response
        /// </summary>
        public string Body { get; protected set; } = null;

        /// <summary>
        /// Gets the original content that was returned in the response, if the response returned was Bad
        /// </summary>
        public HttpContent OriginalContent => Response?.Content;

        /// <summary>
        /// Gets the original response from the rest service
        /// </summary>
        public HttpResponseMessage Response { get; private set; }
    }
}
