using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace PusherServer.RestfulClient
{
    /// <summary>
    /// A client for the Pusher REST requests
    /// </summary>
    public class PusherRestClient : IPusherRestClient
    {
        private readonly string _libraryName;
        private readonly string _version;
        private readonly HttpClient _httpClient;

        /// <summary>
        /// Constructs a new instance of the PusherRestClient
        /// </summary>
        /// <param name="baseAddress">The base address of the Pusher API as a URI formatted string</param>
        /// <param name="libraryName"></param>
        /// <param name="version"></param>
        public PusherRestClient(string baseAddress, string libraryName, Version version) : this(new Uri(baseAddress), libraryName, version)
        {}

        /// <summary>
        /// Constructs a new instance of the PusherRestClient with a supplied HttpClient
        /// </summary>
        /// <param name="baseAddress">The base address of the Pusher API</param>
        /// <param name="libraryName">The name of the Pusher Library</param>
        /// <param name="version">The version of the Pusher library</param>
        public PusherRestClient(Uri baseAddress, string libraryName, Version version):this(CreateDefaultHttpClient(baseAddress), libraryName, version)
        {}

        private static HttpClient CreateDefaultHttpClient(Uri baseAddress)
        {
            return new HttpClient 
            { 
                BaseAddress = baseAddress,
                Timeout = TimeSpan.FromSeconds(30),
            };
        }
        
        /// <summary>
        /// Constructs a new instance of the PusherRestClient with a supplied HttpClient
        /// </summary>
        /// <param name="httpClient">An externally configured HttpClient</param>
        /// <param name="libraryName">The name of the Pusher Library</param>
        /// <param name="version">The version of the Pusher library</param>
        public PusherRestClient(HttpClient httpClient, string libraryName, Version version)
        {
            _httpClient = httpClient;
            _libraryName = libraryName;
            _version = version.ToString(3);
            _httpClient.DefaultRequestHeaders.Clear();
            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            _httpClient.DefaultRequestHeaders.Add("Pusher-Library-Name", _libraryName);
            _httpClient.DefaultRequestHeaders.Add("Pusher-Library-Version", _version);
        }

        ///<inheritDoc/>
        public Uri BaseUrl {
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
        public async Task<GetResult<T>> ExecuteGetAsync<T>(IPusherRestRequest pusherRestRequest)
        {
            GetResult<T> result = null;
            if (pusherRestRequest.Method == PusherMethod.GET)
            {
                var requestMessage = new HttpRequestMessage(HttpMethod.Get, pusherRestRequest.ResourceUri);

                if (pusherRestRequest.Headers != null)
                {
                    foreach (var header in pusherRestRequest.Headers)
                    {
                        requestMessage.Headers.TryAddWithoutValidation(header.Key, header.Value);
                    }
                }

                var response = await _httpClient.SendAsync(requestMessage).ConfigureAwait(false);
                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                result = new GetResult<T>(response, responseContent);
            }

            return result;
        }

        ///<inheritDoc/>
        public async Task<TriggerResult> ExecutePostAsync(IPusherRestRequest pusherRestRequest)
        {
            TriggerResult result = null;
            if (pusherRestRequest.Method == PusherMethod.POST)
            {
                var rawResult = await ExecutePostRawAsync(pusherRestRequest).ConfigureAwait(false);

                result = new TriggerResult(rawResult.Response, rawResult.Body);
            }

            return result;
        }

        ///<inheritDoc/>
        public async Task<RawRequestResult> ExecutePostRawAsync(IPusherRestRequest pusherRestRequest)
        {
            RawRequestResult result = null;
            if (pusherRestRequest.Method == PusherMethod.POST)
            {
                var requestMessage = new HttpRequestMessage(HttpMethod.Post, pusherRestRequest.ResourceUri)
                {
                    Content = new StringContent(pusherRestRequest.GetContentAsJsonString(), Encoding.UTF8, "application/json"),
                };

                if (pusherRestRequest.Headers != null)
                {
                    foreach (var header in pusherRestRequest.Headers)
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
        public async Task<TriggerResult> ExecuteDeleteAsync(IPusherRestRequest pusherRestRequest)
        {
            TriggerResult result = null;
            if (pusherRestRequest.Method == PusherMethod.DELETE)
            {
                var rawResult = await ExecuteDeleteRawAsync(pusherRestRequest).ConfigureAwait(false);

                result = new TriggerResult(rawResult.Response, rawResult.Body);
            }

            return result;
        }

        ///<inheritDoc/>
        public async Task<RawRequestResult> ExecuteDeleteRawAsync(IPusherRestRequest pusherRestRequest)
        {
            RawRequestResult result = null;
            if (pusherRestRequest.Method == PusherMethod.DELETE)
            {
                var requestMessage = new HttpRequestMessage(HttpMethod.Delete, pusherRestRequest.ResourceUri);

                if (pusherRestRequest.Headers != null)
                {
                    foreach (var header in pusherRestRequest.Headers)
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
