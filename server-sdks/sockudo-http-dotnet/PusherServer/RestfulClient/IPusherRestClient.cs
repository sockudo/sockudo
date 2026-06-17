using System;
using System.Threading.Tasks;

namespace PusherServer.RestfulClient
{
    /// <summary>
    /// Contract for a client for the Pusher REST requests
    /// </summary>
    public interface IPusherRestClient
    {
        /// <summary>
        /// Execute a REST GET request to the Pusher API asynchronously
        /// </summary>
        /// <param name="pusherRestRequest">The request to execute</param>
        /// <returns>The response received from Pusher</returns>
        Task<GetResult<T>> ExecuteGetAsync<T>(IPusherRestRequest pusherRestRequest);

        /// <summary>
        /// Execute a REST POST request to the Pusher API asynchronously
        /// </summary>
        /// <param name="pusherRestRequest">The request to execute</param>
        /// <returns>The response received from Pusher</returns>
        Task<TriggerResult> ExecutePostAsync(IPusherRestRequest pusherRestRequest);

        /// <summary>
        /// Execute a REST POST request to the Pusher API asynchronously without trigger-response parsing.
        /// </summary>
        /// <param name="pusherRestRequest">The request to execute</param>
        /// <returns>The raw response received from Pusher</returns>
        Task<RawRequestResult> ExecutePostRawAsync(IPusherRestRequest pusherRestRequest);

        /// <summary>
        /// Execute a REST DELETE request to the Pusher API asynchronously
        /// </summary>
        /// <param name="pusherRestRequest">The request to execute</param>
        /// <returns>The response received from Pusher</returns>
        Task<TriggerResult> ExecuteDeleteAsync(IPusherRestRequest pusherRestRequest);

        /// <summary>
        /// Execute a REST DELETE request to the Pusher API asynchronously without trigger-response parsing.
        /// </summary>
        /// <param name="pusherRestRequest">The request to execute</param>
        /// <returns>The raw response received from Pusher</returns>
        Task<RawRequestResult> ExecuteDeleteRawAsync(IPusherRestRequest pusherRestRequest);

        /// <summary>
        /// Gets the Base Url this client is using
        /// </summary>
        Uri BaseUrl { get; }

        /// <summary>
        /// Gets or sets the Pusher rest client timeout. The default timeout is 30 seconds.
        /// </summary>
        TimeSpan Timeout { get; set; }
    }
}
