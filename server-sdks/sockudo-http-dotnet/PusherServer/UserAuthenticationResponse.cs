using System;
using System.Runtime.Serialization;

namespace PusherServer
{
    [DataContract]
    class UserAuthenticationResponse: IUserAuthenticationResponse
    {
        private readonly string _appKey;
        private readonly string _appSecret;
        private readonly string _socketId;
        private readonly UserData _userData;

        public UserAuthenticationResponse(string appKey, string appSecret, string socketId, UserData userData)
        {
            ValidationHelper.ValidateSocketId(socketId);
            if (userData == null)
            {
                throw new ArgumentNullException(nameof(userData));
            }

            _appKey = appKey;
            _appSecret = appSecret;
            _socketId = socketId;
            _userData = userData;
        }

        [DataMember(Name = "auth", IsRequired = true)]
        public string auth
        {
            get
            {
                var userDataJson = DefaultSerializer.Default.Serialize(_userData);
                var stringToSign = _socketId + "::user::" + userDataJson;
                
                return _appKey + ":" + CryptoHelper.GetHmac256(_appSecret, stringToSign);
            }
        }

        /// <summary>
        /// Double encoded JSON containing user information.
        /// </summary>
        [DataMember(Name = "user_data", IsRequired = false, EmitDefaultValue = false)]
        public string user_data
        {
            get
            {
                return DefaultSerializer.Default.Serialize(_userData);
            }
        }

        public string ToJson()
        {
            return ToString();
        }

        public override string ToString()
        {
            return DefaultSerializer.Default.Serialize(this);
        }
    }
}
