namespace PusherServer.RestfulClient
{
    /// <summary>
    /// Enum used to determine what kind of REST request to make
    /// </summary>
    public enum PusherMethod
    {
        /// <summary>
        /// Make a GET request
        /// </summary>
        GET,
        /// <summary>
        /// Make a POST request
        /// </summary>
        POST,
        /// <summary>
        /// Make a DELETE request
        /// </summary>
        DELETE
    }
}
