using System;
using System.Threading.Tasks;

namespace SockudoServer
{
    /// <summary>
    /// Provides access to functionality within the Sockudo service such as Trigger to trigger events
    /// and authenticating subscription requests to private and presence channels.
    /// </summary>
    public interface ISockudo
    {
        /// <summary>
        /// Triggers an event on the specified channels in the background.
        /// </summary>
        /// <param name="channelName">The name of the channel to trigger the event on</param>
        /// <param name="eventName">The name of the event.</param>
        /// <param name="data">The data to be sent with the event. The event payload.</param>
        /// <param name="options">Additional options to be used when triggering the event. See <see cref="ITriggerOptions" />.</param>
        /// <returns>The result of the call to the REST API</returns>
        Task<ITriggerResult> TriggerAsync(string channelName, string eventName, object data, ITriggerOptions options = null);

        /// <summary>
        /// Triggers an event on the specified channels in the background.
        /// </summary>
        /// <param name="channelNames">The channels to trigger the event on</param>
        /// <param name="eventName">The name of the event.</param>
        /// <param name="data">The data to be sent with the event. The event payload.</param>
        /// <param name="options">(Optional)Additional options to be used when triggering the event. See <see cref="ITriggerOptions" />.</param>
        /// <returns>The result of the call to the REST API</returns>
        Task<ITriggerResult> TriggerAsync(string[] channelNames, string eventName, object data, ITriggerOptions options = null);

        /// <summary>
        /// Triggers the events in the passed in array asynchronously
        /// </summary>
        /// <param name="events">The events to trigger</param>
        /// <returns>The result of the call to the REST API</returns>
        Task<ITriggerResult> TriggerAsync(Event[] events);

        /// <summary>
        /// DEPRECATED: Use <see cref="AuthorizeChannel" /> instead.
        /// Authorizes the subscription request for a private channel.
        /// </summary>
        /// <param name="channelName">Name of the channel to be authenticated.</param>
        /// <param name="socketId">The socket id which uniquely identifies the connection attempting to subscribe to the channel.</param>
        /// <returns>
        /// Authorization response where the required auth token can be accessed via <see cref="IChannelAuthorizationResponse.auth"/>
        /// The full response can be accessed via <see cref="IChannelAuthorizationResponse.ToJson()"/>
        /// </returns>
        IAuthenticationData Authenticate(string channelName, string socketId);

        /// <summary>
        /// DEPRECATED: Use <see cref="AuthorizeChannel" /> instead.
        /// Authorizes the subscription request for a presence channel.
        /// </summary>
        /// <param name="channelName">Name of the channel to be authenticated.</param>
        /// <param name="socketId">The socket id which uniquely identifies the connection attempting to subscribe to the channel.</param>
        /// <param name="data">Information about the user subscribing to the presence channel.</param>
        /// <exception cref="ArgumentNullException">If <paramref name="data"/> is null</exception>
        /// <returns>
        /// Authorization response where the required auth token can be accessed via <see cref="IChannelAuthorizationResponse.auth"/>
        /// The full response can be accessed via <see cref="IChannelAuthorizationResponse.ToJson()"/>
        /// </returns>
        IAuthenticationData Authenticate(string channelName, string socketId, PresenceChannelData data);

        /// <summary>
        /// Authorizes the subscription request for a private channel.
        /// </summary>
        /// <param name="channelName">Name of the channel to be authenticated.</param>
        /// <param name="socketId">The socket id which uniquely identifies the connection attempting to subscribe to the channel.</param>
        /// <returns>
        /// Authorization response where the required auth token can be accessed via <see cref="IChannelAuthorizationResponse.auth"/>
        /// The full response can be accessed via <see cref="IChannelAuthorizationResponse.ToJson()"/>
        /// </returns>
        IChannelAuthorizationResponse AuthorizeChannel(string channelName, string socketId);

        /// <summary>
        /// Authorizes the subscription request for a presence channel.
        /// </summary>
        /// <param name="channelName">Name of the channel to be authenticated.</param>
        /// <param name="socketId">The socket id which uniquely identifies the connection attempting to subscribe to the channel.</param>
        /// <param name="data">Information about the user subscribing to the presence channel.</param>
        /// <exception cref="ArgumentNullException">If <paramref name="data"/> is null</exception>
        /// <returns>
        /// Authorization response where the required auth token can be accessed via <see cref="IChannelAuthorizationResponse.auth"/>
        /// The full response can be accessed via <see cref="IChannelAuthorizationResponse.ToJson()"/>
        /// </returns>
        IChannelAuthorizationResponse AuthorizeChannel(string channelName, string socketId, PresenceChannelData data);

        /// <summary>
        /// Authenticates a user to Sockudo.
        /// </summary>
        /// <param name="socketId">The socket id which uniquely identifies the connection attempting to authenticate a user.</param>
        /// <param name="userData">Information about the user.</param>
        /// <exception cref="ArgumentNullException">If <paramref name="userData"/> is null</exception>
        /// <returns>
        /// Authentication response where the required auth token can be accessed via <see cref="IUserAuthenticationResponse.auth"/>
        /// The full response can be accessed via <see cref="IUserAuthenticationResponse.ToJson()"/>
        /// </returns>
        IUserAuthenticationResponse AuthenticateUser(string socketId, UserData userData);


        /// <summary>
        /// Makes an asynchronous GET request to the specified resource. Authentication is handled as part of the call. The data returned from the request is deserizlized to the object type defined by <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="resource">The resource.</param>
        /// <param name="parameters">Additional parameters to be sent as part of the request query string.</param>
        /// <returns>The result of the GET request</returns>
        Task<IGetResult<T>> GetAsync<T>(string resource, object parameters = null);

        /// <summary>
        /// Handle an incoming WebHook and validate it.
        /// </summary>
        /// <param name="signature">The signature of the incoming WebHook</param>
        /// <param name="body">The body of the incoming Webhook request</param>
        /// <returns>A WebHook helper.</returns>
        IWebHook ProcessWebHook(string signature, string body);

        /// <summary>
        /// Queries the Sockudo API for the Users of a Presence Channel asynchronously
        /// </summary>
        /// <typeparam name="T">The type of object that will be returned by the API</typeparam>
        /// <param name="channelName">The name of the channel to query</param>
        /// <returns>The result of the Presence Channel Users query</returns>
        Task<IGetResult<T>> FetchUsersFromPresenceChannelAsync<T>(string channelName);

        /// <summary>
        /// Asynchronously queries the Sockudo API for the state of a Channel
        /// </summary>
        /// <typeparam name="T">The type of object that will be returned by the API</typeparam>
        /// <param name="channelName">The name of the channel to query</param>
        /// <param name="info">An object containing a list of attributes to include in the query</param>
        /// <returns>The result of the Channel State query</returns>
        Task<IGetResult<T>> FetchStateForChannelAsync<T>(string channelName, object info = null);

        /// <summary>
        /// Queries the Sockudo API for the state of all channels based upon the info object
        /// </summary>
        /// <typeparam name="T">The type of object that will be returned by the API</typeparam>
        /// <param name="info">An object containing a list of attributes to include in the query</param>
        Task<IGetResult<T>> FetchStateForChannelsAsync<T>(object info);

        /// <summary>
        /// Queries durable history for a channel asynchronously.
        /// </summary>
        /// <typeparam name="T">The type of object that will be returned by the API</typeparam>
        /// <param name="channelName">The name of the channel to query</param>
        /// <param name="parameters">History query parameters such as limit, direction, cursor, start_serial, end_serial, start_time_ms and end_time_ms</param>
        Task<IGetResult<T>> FetchHistoryForChannelAsync<T>(string channelName, object parameters = null);

        /// <summary>
        /// Fetches the latest visible version of a mutable message asynchronously.
        /// </summary>
        Task<IGetResult<T>> GetMessageAsync<T>(string channelName, string messageSerial);

        /// <summary>
        /// Fetches preserved versions of a mutable message asynchronously.
        /// </summary>
        Task<IGetResult<T>> GetMessageVersionsAsync<T>(string channelName, string messageSerial, object parameters = null);

        /// <summary>
        /// Applies a mutable-message update asynchronously.
        /// </summary>
        Task<IGetResult<T>> UpdateMessageAsync<T>(string channelName, string messageSerial, object body);

        /// <summary>
        /// Applies a mutable-message delete asynchronously.
        /// </summary>
        Task<IGetResult<T>> DeleteMessageAsync<T>(string channelName, string messageSerial, object body = null);

        /// <summary>
        /// Applies a mutable-message append asynchronously.
        /// </summary>
        Task<IGetResult<T>> AppendMessageAsync<T>(string channelName, string messageSerial, object body);

        /// <summary>
        /// Publishes an annotation for a versioned message asynchronously.
        /// </summary>
        Task<IGetResult<T>> PublishAnnotationAsync<T>(string channelName, string messageSerial, object body);

        /// <summary>
        /// Deletes an annotation from a versioned message asynchronously.
        /// </summary>
        Task<IGetResult<T>> DeleteAnnotationAsync<T>(string channelName, string messageSerial, string annotationSerial, object parameters = null);

        /// <summary>
        /// Lists raw annotation events for a versioned message asynchronously.
        /// </summary>
        Task<IGetResult<T>> ListAnnotationsAsync<T>(string channelName, string messageSerial, object parameters = null);

        /// <summary>
        /// Queries presence history for a presence channel asynchronously.
        /// </summary>
        /// <typeparam name="T">The type of object that will be returned by the API</typeparam>
        /// <param name="channelName">The presence channel to query</param>
        /// <param name="parameters">Presence history query parameters such as limit, direction, cursor, start_serial, end_serial, start_time_ms and end_time_ms</param>
        Task<IGetResult<T>> FetchPresenceHistoryForChannelAsync<T>(string channelName, object parameters = null);

        /// <summary>
        /// Queries a reconstructed presence snapshot for a presence channel asynchronously.
        /// </summary>
        /// <typeparam name="T">The type of object that will be returned by the API</typeparam>
        /// <param name="channelName">The presence channel to query</param>
        /// <param name="parameters">Snapshot query parameters such as at_time_ms and at_serial</param>
        Task<IGetResult<T>> FetchPresenceSnapshotForChannelAsync<T>(string channelName, object parameters = null);

        Task<IGetResult<T>> ActivateDeviceAsync<T>(object device);
        Task<IGetResult<T>> CreateDeviceActivationAsync<T>(object device);
        Task<IGetResult<T>> UpdateDeviceRegistrationAsync<T>(object device, string deviceIdentityToken);
        Task<IGetResult<T>> ListDeviceRegistrationsAsync<T>(object parameters = null);
        Task<IGetResult<T>> GetDeviceRegistrationAsync<T>(string deviceId, string deviceIdentityToken = null);
        Task<IGetResult<T>> DeleteDeviceRegistrationAsync<T>(string deviceId, string deviceIdentityToken = null);
        Task<IGetResult<T>> RemoveDeviceRegistrationsByClientAsync<T>(string clientId);
        Task<IGetResult<T>> UpsertChannelPushSubscriptionAsync<T>(object subscription, string deviceIdentityToken = null);
        Task<IGetResult<T>> ListChannelPushSubscriptionsAsync<T>(object parameters = null, string deviceIdentityToken = null);
        Task<IGetResult<T>> DeleteChannelPushSubscriptionsAsync<T>(object parameters, string deviceIdentityToken = null);
        Task<IGetResult<T>> ListChannelPushSubscriptionChannelsAsync<T>(object parameters = null);
        Task<IGetResult<T>> ListPushCredentialsAsync<T>(object parameters = null);
        Task<IGetResult<T>> PutPushCredentialAsync<T>(string provider, object credential);
        Task<IGetResult<T>> PublishPushAsync<T>(object request);
        Task<IGetResult<T>> PublishPushDirectAsync<T>(object request);
        Task<IGetResult<T>> PublishPushBatchAsync<T>(object[] requests);
        Task<IGetResult<T>> SchedulePushAsync<T>(object request);
        Task<IGetResult<T>> GetPublishStatusAsync<T>(string publishId);
        Task<IGetResult<T>> CancelScheduledPushAsync<T>(string publishId);
        Task<IGetResult<T>> PostPushDeliveryStatusAsync<T>(object deliveryEvent);

        /// <summary>
        /// Terminates all active connections for the specified user.
        /// </summary>
        /// <param name="userId">The ID of the user whose connections should be terminated.</param>
        /// <returns>The result of the call to the REST API</returns>
        Task<IGetResult<object>> TerminateUserConnectionsAsync(string userId);

        /// <summary>
        /// Forces all active connections for the specified user to reconnect by closing them with code 4200.
        /// </summary>
        /// <param name="userId">The ID of the user whose connections should be forced to reconnect.</param>
        /// <returns>The result of the call to the REST API</returns>
        Task<IGetResult<object>> ForceReconnectUserAsync(string userId);
    }
}
