using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace SockudoServer.RestfulClient
{
    /// <summary>
    /// A client for the Sockudo REST requests
    /// </summary>
    public class SockudoRestClient : ISockudoRestClient
    {
        private readonly string _libraryName;
        private readonly string _version;
        private readonly HttpClient _httpClient;

        /// <summary>
        /// Constructs a new instance of the SockudoRestClient
        /// </summary>
        /// <param name="baseAddress">The base address of the Sockudo API as a URI formatted string</param>
        /// <param name="libraryName"></param>
        /// <param name="version"></param>
        public SockudoRestClient(string baseAddress, string libraryName, Version version) : this(new Uri(baseAddress), libraryName, version)
        { }

        /// <summary>
        /// Constructs a new instance of the SockudoRestClient with a supplied HttpClient
        /// </summary>
        /// <param name="baseAddress">The base address of the Sockudo API</param>
        /// <param name="libraryName">The name of the Sockudo Library</param>
        /// <param name="version">The version of the Sockudo library</param>
        public SockudoRestClient(Uri baseAddress, string libraryName, Version version) : this(CreateDefaultHttpClient(baseAddress), libraryName, version)
        { }

        private static HttpClient CreateDefaultHttpClient(Uri baseAddress)
        {
            return new HttpClient
            {
                BaseAddress = baseAddress,
                Timeout = TimeSpan.FromSeconds(30),
            };
        }

        /// <summary>
        /// Constructs a new instance of the SockudoRestClient with a supplied HttpClient
        /// </summary>
        /// <param name="httpClient">An externally configured HttpClient</param>
        /// <param name="libraryName">The name of the Sockudo Library</param>
        /// <param name="version">The version of the Sockudo library</param>
        public SockudoRestClient(HttpClient httpClient, string libraryName, Version version)
        {
            _httpClient = httpClient;
            _libraryName = libraryName;
            _version = version.ToString(3);
            _httpClient.DefaultRequestHeaders.Clear();
            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            _httpClient.DefaultRequestHeaders.Add("Sockudo-Library-Name", _libraryName);
            _httpClient.DefaultRequestHeaders.Add("Sockudo-Library-Version", _version);
        }

        ///<inheritDoc/>
        public Uri BaseUrl
        {
            get { return _httpClient.BaseAddress; }
        }

        ///<inheritDoc/>
        public TimeSpan Timeout
        {
            get
            {
                return _httpClient.Timeout;
            }

            set
            {
                _httpClient.Timeout = value;
            }
        }

        ///<inheritDoc/>
        public async Task<GetResult<T>> ExecuteGetAsync<T>(ISockudoRestRequest sockudoRestRequest)
        {
            GetResult<T> result = null;
            if (sockudoRestRequest.Method == SockudoMethod.GET)
            {
                var response = await _httpClient.GetAsync(sockudoRestRequest.ResourceUri).ConfigureAwait(false);
                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                result = new GetResult<T>(response, responseContent);
            }

            return result;
        }

        ///<inheritDoc/>
        public async Task<TriggerResult> ExecutePostAsync(ISockudoRestRequest sockudoRestRequest)
        {
            TriggerResult result = null;
            if (sockudoRestRequest.Method == SockudoMethod.POST)
            {
                var rawResult = await ExecutePostRawAsync(sockudoRestRequest).ConfigureAwait(false);

                result = new TriggerResult(rawResult.Response, rawResult.Body);
            }

            return result;
        }

        ///<inheritDoc/>
        public async Task<RawRequestResult> ExecutePostRawAsync(ISockudoRestRequest sockudoRestRequest)
        {
            RawRequestResult result = null;
            if (sockudoRestRequest.Method == SockudoMethod.POST)
            {
                var requestMessage = new HttpRequestMessage(HttpMethod.Post, sockudoRestRequest.ResourceUri)
                {
                    Content = new StringContent(sockudoRestRequest.GetContentAsJsonString(), Encoding.UTF8, "application/json"),
                };

                if (sockudoRestRequest.Headers != null)
                {
                    foreach (var header in sockudoRestRequest.Headers)
                    {
                        requestMessage.Headers.TryAddWithoutValidation(header.Key, header.Value);
                    }
                }

                var response = await _httpClient.SendAsync(requestMessage).ConfigureAwait(false);
                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                result = new RawRequestResult(response, responseContent);
            }

            return result;
        }

        ///<inheritDoc/>
        public async Task<TriggerResult> ExecuteDeleteAsync(ISockudoRestRequest sockudoRestRequest)
        {
            TriggerResult result = null;
            if (sockudoRestRequest.Method == SockudoMethod.DELETE)
            {
                var rawResult = await ExecuteDeleteRawAsync(sockudoRestRequest).ConfigureAwait(false);

                result = new TriggerResult(rawResult.Response, rawResult.Body);
            }

            return result;
        }

        ///<inheritDoc/>
        public async Task<RawRequestResult> ExecuteDeleteRawAsync(ISockudoRestRequest sockudoRestRequest)
        {
            RawRequestResult result = null;
            if (sockudoRestRequest.Method == SockudoMethod.DELETE)
            {
                var requestMessage = new HttpRequestMessage(HttpMethod.Delete, sockudoRestRequest.ResourceUri);

                if (sockudoRestRequest.Headers != null)
                {
                    foreach (var header in sockudoRestRequest.Headers)
                    {
                        requestMessage.Headers.TryAddWithoutValidation(header.Key, header.Value);
                    }
                }

                var response = await _httpClient.SendAsync(requestMessage).ConfigureAwait(false);
                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                result = new RawRequestResult(response, responseContent);
            }

            return result;
        }
    }
}
