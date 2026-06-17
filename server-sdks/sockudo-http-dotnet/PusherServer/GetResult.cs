using System.Net.Http;
using Newtonsoft.Json;

namespace PusherServer
{
    /// <summary>
    /// Deserialised the result from a Rest Response
    /// </summary>
    /// <typeparam name="T">The Type the Rest Response contains</typeparam>
    public class GetResult<T> : RequestResult, IGetResult<T>
    {
        /// <summary>
        /// Attempts to deserialise the data contained with a Rest Response
        /// </summary>
        /// <param name="response">The original response from the rest call</param>
        /// <param name="content">the extracted content</param>
        public GetResult(HttpResponseMessage response, string content) : base(response, content)
        {
            if (response.IsSuccessStatusCode)
                Data = JsonConvert.DeserializeObject<T>(content);
        }

        /// <summary>
        /// Gets the data deserialised from the Rest Response
        /// </summary>
        public T Data { get; }
    }
}
