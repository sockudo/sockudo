using System;
using System.Collections.Generic;
using System.Reflection;

namespace PusherServer.RestfulClient
{
    /// <summary>
    /// Factory that creates authenticated requests to send to the Pusher API
    /// </summary>
    public class AuthenticatedRequestFactory : IAuthenticatedRequestFactory
    {
        private readonly string _appKey;
        private readonly string _appId;
        private readonly string _appSecret;

        /// <summary>
        /// Constructs a new Autheticated Request Factory
        /// </summary>
        /// <param name="appKey">Your app Key for the Pusher API</param>
        /// <param name="appId">Your app Id for the Pusher API</param>
        /// <param name="appSecret">Your app Secret for the Pusher API</param>
        public AuthenticatedRequestFactory(string appKey, string appId, string appSecret)
        {
            _appKey = appKey;
            _appId = appId;
            _appSecret = appSecret;
        }

        /// <inheritdoc/>
        public IPusherRestRequest Build(PusherMethod requestType, string resource, object requestParameters = null, object requestBody = null)
        {
            SortedDictionary<string, string> queryParams = GetQueryString(requestParameters, requestBody);
            string queryString = BuildQueryString(queryParams);
            string queryStringForSigning = BuildQueryString(queryParams, lowercaseKeys: true);

            string path = $"/apps/{_appId}/{resource.TrimStart('/')}";

            string authToSign = String.Format(
                Enum.GetName(requestType.GetType(), requestType) +
                "\n{0}\n{1}",
                path,
                queryStringForSigning);

            string authSignature = CryptoHelper.GetHmac256(_appSecret, authToSign);

            string requestUrl = $"{path}?auth_signature={authSignature}&{queryString}";

            IPusherRestRequest request = new PusherRestRequest(requestUrl)
            {
                Method = requestType,
                Body = requestBody,
            };

            return request;
        }

        private SortedDictionary<string, string> GetQueryString(object requestParameters, object requestBody)
        {
            SortedDictionary<string, string> parameters = GetStringBuilderfromSourceObject(requestParameters);

            int timeNow = (int)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;

            parameters.Add("auth_key", _appKey);
            parameters.Add("auth_timestamp", timeNow.ToString());
            parameters.Add("auth_version", "1.0");

            if (requestBody != null)
            {
                var bodyDataJson = DefaultSerializer.Default.Serialize(requestBody);
                var bodyMd5 = CryptoHelper.GetMd5Hash(bodyDataJson);
                parameters.Add("body_md5", bodyMd5);
            }
            
            return parameters;
        }

        private static string BuildQueryString(SortedDictionary<string, string> queryParams, bool lowercaseKeys = false)
        {
            string queryString = string.Empty;
            foreach (KeyValuePair<string, string> parameter in queryParams)
            {
                string key = lowercaseKeys ? parameter.Key.ToLowerInvariant() : parameter.Key;
                queryString += key + "=" + parameter.Value + "&";
            }

            return queryString.TrimEnd('&');
        }

        private static SortedDictionary<string, string> GetStringBuilderfromSourceObject(object sourceObject)
        {
            SortedDictionary<string, string> parameters = new SortedDictionary<string, string>();

            if (sourceObject != null)
            {
                Type objType = sourceObject.GetType();
                IList<PropertyInfo> propertyInfos = new List<PropertyInfo>(objType.GetTypeInfo().DeclaredProperties);

                foreach (PropertyInfo propertyInfo in propertyInfos)
                {
                    parameters.Add(propertyInfo.Name, propertyInfo.GetValue(sourceObject, null).ToString());
                }
            }

            return parameters;
        }
    }
}
