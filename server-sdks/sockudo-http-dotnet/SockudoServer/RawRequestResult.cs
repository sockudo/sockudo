using System.Net.Http;

namespace SockudoServer
{
    /// <summary>
    /// Raw request result for non-trigger POST/DELETE admin endpoints.
    /// </summary>
    public class RawRequestResult : RequestResult
    {
        public RawRequestResult(HttpResponseMessage response, string originalContent) : base(response, originalContent)
        {
        }
    }
}
