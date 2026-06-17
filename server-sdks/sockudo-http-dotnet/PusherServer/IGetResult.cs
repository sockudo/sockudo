namespace PusherServer
{
    /// <summary>
    /// The result of a GET HTTP request to the Pusher REST API.
    /// </summary>
    /// <typeparam name="T">The object type that the data returned from the request should be deserialized to.</typeparam>
    public interface IGetResult<T>: IRequestResult
    {
        /// <summary>
        /// Gets the data returned from the request in a deserialized form.
        /// </summary>
        /// <value>
        /// The data.
        /// </value>
        T Data { get;  }
    }
}
