using System;
using System.Threading.Tasks;

namespace SockudoServer.RestfulClient
{
    /// <summary>
    /// Contract for a client for the Sockudo REST requests
    /// </summary>
    public interface ISockudoRestClient
    {
        /// <summary>
        /// Execute a REST GET request to the Sockudo API asynchronously
        /// </summary>
        /// <param name="sockudoRestRequest">The request to execute</param>
        /// <returns>The response received from Sockudo</returns>
        Task<GetResult<T>> ExecuteGetAsync<T>(ISockudoRestRequest sockudoRestRequest);

        /// <summary>
        /// Execute a REST POST request to the Sockudo API asynchronously
        /// </summary>
        /// <param name="sockudoRestRequest">The request to execute</param>
        /// <returns>The response received from Sockudo</returns>
        Task<TriggerResult> ExecutePostAsync(ISockudoRestRequest sockudoRestRequest);

        /// <summary>
        /// Execute a REST POST request to the Sockudo API asynchronously without trigger-response parsing.
        /// </summary>
        /// <param name="sockudoRestRequest">The request to execute</param>
        /// <returns>The raw response received from Sockudo</returns>
        Task<RawRequestResult> ExecutePostRawAsync(ISockudoRestRequest sockudoRestRequest);

        /// <summary>
        /// Execute a REST DELETE request to the Sockudo API asynchronously
        /// </summary>
        /// <param name="sockudoRestRequest">The request to execute</param>
        /// <returns>The response received from Sockudo</returns>
        Task<TriggerResult> ExecuteDeleteAsync(ISockudoRestRequest sockudoRestRequest);

        /// <summary>
        /// Execute a REST DELETE request to the Sockudo API asynchronously without trigger-response parsing.
        /// </summary>
        /// <param name="sockudoRestRequest">The request to execute</param>
        /// <returns>The raw response received from Sockudo</returns>
        Task<RawRequestResult> ExecuteDeleteRawAsync(ISockudoRestRequest sockudoRestRequest);

        /// <summary>
        /// Gets the Base Url this client is using
        /// </summary>
        Uri BaseUrl { get; }

        /// <summary>
        /// Gets or sets the Sockudo rest client timeout. The default timeout is 30 seconds.
        /// </summary>
        TimeSpan Timeout { get; set; }
    }
}
