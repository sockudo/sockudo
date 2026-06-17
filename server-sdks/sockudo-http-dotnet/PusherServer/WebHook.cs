using System;
using System.Collections.Generic;
using System.Linq;

namespace PusherServer
{
    internal class WebHook: IWebHook
    {
        private readonly WebHookData _webHookData;
        private readonly List<string> _validationErrors;
        private IDeserializeJsonStrings _jsonDeserializer;

        internal WebHook(string secret, string signature, string body)
        {
            if(string.IsNullOrEmpty(secret))
            {
                throw new ArgumentException("A secret must be provided", "secret");
            }

            this._validationErrors = new List<string>();

            this._webHookData = ValidateWebHook(secret, signature, body);
        }

        private WebHookData ValidateWebHook(string secret, string signature, string body)
        {
            WebHookData parsedWebHookData = null;

            var signatureNullOrEmpth = string.IsNullOrEmpty(signature);
            if(signatureNullOrEmpth == true) {
                this._validationErrors.Add("The supplied signature to check was null or empty. A signature to check must be provided.");
            }

            if (string.IsNullOrEmpty(body) == true)
            {
                this._validationErrors.Add("The supplied body to check was null or empty. A body to check must be provided.");
            }
            else
            {
                try
                {
                    parsedWebHookData = JsonDeserializer.Deserialize<WebHookData>(body);
                }
                catch (Exception e)
                {
                    var validationError = string.Format("Exception occurred parsing the body as JSON: {0}{1}", Environment.NewLine, e.StackTrace);
                    this._validationErrors.Add(validationError);
                }
            }

            if (parsedWebHookData != null)
            {
                var expectedSignature = CryptoHelper.GetHmac256(secret, body);
                var signatureIsValid = (signature == expectedSignature);
                if (signatureIsValid == false)
                {
                    this._validationErrors.Add(
                        string.Format("The signature did not validate. Expected {0}. Got {1}", signature, expectedSignature)
                    );
                }
            }
            return parsedWebHookData;
        }

        public bool IsValid
        {
            get
            {
                return (this.ValidationErrors.Length == 0);
            }
        }

        public Dictionary<string, string>[] Events
        {
            get
            {
                return this._webHookData.events;
            }
        }

        public DateTime Time
        {
            get
            {
                return this._webHookData.Time;
            }
        }

        public string[] ValidationErrors
        {
            get
            {
                return this._validationErrors.ToArray<string>();
            }
        }

        /// <summary>
        /// Gets or sets the JSON Deserializer to use
        /// </summary>
        public IDeserializeJsonStrings JsonDeserializer
        {
            get
            {
                if (_jsonDeserializer == null)
                {
                    _jsonDeserializer = new DefaultDeserializer();
                }

                return _jsonDeserializer;
            }
            set { _jsonDeserializer = value; }
        }
    }
}
