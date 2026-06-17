<?php

namespace Sockudo;

use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Promise\PromiseInterface;

interface SockudoInterface
{
    /**
     * Fetch the settings.
     *
     * @return array
     */
    public function getSettings();

    /**
     * Trigger an event by providing event name and payload.
     * Optionally provide a socket ID to exclude a client (most likely the sender).
     *
     * @param array|string $channels        A channel name or an array of channel names to publish the event on.
     * @param string       $event
     * @param mixed        $data            Event data
     * @param array        $params          [optional]
     * @param bool         $already_encoded [optional]
     *
     * @throws SockudoException   Throws exception if $channels is an array of size 101 or above or $socket_id is invalid
     * @throws ApiErrorException Throws ApiErrorException if the Channels HTTP API responds with an error
     * @throws GuzzleException
     *
     */
    public function trigger($channels, string $event, $data, array $params = [], bool $already_encoded = false): object;

    /**
     * Asynchronously trigger an event by providing event name and payload.
     * Optionally provide a socket ID to exclude a client (most likely the sender).
     *
     * @param array|string $channels        A channel name or an array of channel names to publish the event on.
     * @param mixed        $data            Event data
     * @param array        $params          [optional]
     * @param bool         $already_encoded [optional]
     *
     */
    public function triggerAsync($channels, string $event, $data, array $params = [], bool $already_encoded = false): PromiseInterface;

    /**
     * Trigger multiple events at the same time.
     *
     * @param array $batch           [optional] An array of events to send
     * @param bool  $already_encoded [optional]
     *
     * @throws SockudoException   Throws exception if curl wasn't initialized correctly
     * @throws ApiErrorException Throws ApiErrorException if the Channels HTTP API responds with an error
     * @throws GuzzleException
     *
     */
    public function triggerBatch(array $batch = [], bool $already_encoded = false): object;

    /**
     * Asynchronously trigger multiple events at the same time.
     *
     * @param array $batch           [optional] An array of events to send
     * @param bool  $already_encoded [optional]
     *
     * @throws SockudoException   Throws exception if curl wasn't initialized correctly
     * @throws ApiErrorException Throws ApiErrorException if the Channels HTTP API responds with an error
     *
     */
    public function triggerBatchAsync(array $batch = [], bool $already_encoded = false): PromiseInterface;

    /**
     * Get information, such as subscriber and user count, for a channel.
     *
     * @param string $channel The name of the channel
     * @param array  $params  Additional parameters for the query e.g. $params = array( 'info' => 'connection_count' )
     *
     * @throws SockudoException   If $channel is invalid or if curl wasn't initialized correctly
     * @throws ApiErrorException Throws ApiErrorException if the Channels HTTP API responds with an error
     * @throws GuzzleException
     *
     */
    public function getChannelInfo(string $channel, array $params = []): object;

    /**
     * Fetch a list containing all channels.
     *
     * @param array $params Additional parameters for the query e.g. $params = array( 'info' => 'connection_count' )
     *
     * @throws SockudoException   Throws exception if curl wasn't initialized correctly
     * @throws ApiErrorException Throws ApiErrorException if the Channels HTTP API responds with an error
     * @throws GuzzleException
     *
     */
    public function getChannels(array $params = []): object;

    /**
     * Fetch user ids currently subscribed to a presence channel.
     *
     * @param string $channel The name of the channel
     *
     * @throws SockudoException   Throws exception if curl wasn't initialized correctly
     * @throws ApiErrorException Throws ApiErrorException if the Channels HTTP API responds with an error
     * @throws GuzzleException
     *
     */
    public function getPresenceUsers(string $channel): object;

    /**
     * Fetch durable history for a channel.
     *
     * @param string $channel The name of the channel
     * @param array  $params  History query params: limit, direction, cursor,
     *                        start_serial, end_serial, start_time_ms, end_time_ms
     *
     * @throws SockudoException
     * @throws ApiErrorException
     * @throws GuzzleException
     *
     */
    public function getChannelHistory(string $channel, array $params = []): object;

    /**
     * Fetch presence history for a presence channel.
     *
     * @param string $channel The name of the presence channel
     * @param array  $params  Query params: limit, direction, cursor,
     *                        start_serial, end_serial, start_time_ms, end_time_ms
     *
     * @throws SockudoException
     * @throws ApiErrorException
     * @throws GuzzleException
     */
    public function getPresenceHistory(string $channel, array $params = []): object;

    /**
     * Fetch a presence snapshot (reconstructed membership) for a presence channel.
     *
     * @param string $channel The name of the presence channel
     * @param array  $params  Query params: at_time_ms, at_serial
     *
     * @throws SockudoException
     * @throws ApiErrorException
     * @throws GuzzleException
     */
    public function getPresenceSnapshot(string $channel, array $params = []): object;

    /**
     * Fetch the latest visible version of a mutable message.
     */
    public function getMessage(string $channel, string $messageSerial): object;

    /**
     * Fetch preserved versions of a mutable message.
     */
    public function getMessageVersions(string $channel, string $messageSerial, array $params = []): object;

    /**
     * Apply a mutable-message update.
     */
    public function updateMessage(string $channel, string $messageSerial, array $params = []): object;

    /**
     * Apply a mutable-message delete.
     */
    public function deleteMessage(string $channel, string $messageSerial, array $params = []): object;

    /**
     * Apply a mutable-message append.
     */
    public function appendMessage(string $channel, string $messageSerial, array $params = []): object;

    /**
     * Publish an annotation for a versioned message.
     */
    public function publishAnnotation(string $channel, string $messageSerial, array $params): object;

    /**
     * Delete an annotation from a versioned message.
     */
    public function deleteAnnotation(string $channel, string $messageSerial, string $annotationSerial, array $params = []): object;

    /**
     * List raw annotation events for a versioned message.
     */
    public function listAnnotations(string $channel, string $messageSerial, array $params = []): object;

    /**
     * Activate or create a push device registration with admin scope.
     */
    public function activateDevice(array $device, array $options = []): object;

    /**
     * Alias of activateDevice.
     */
    public function createDeviceActivation(array $device, array $options = []): object;

    /**
     * Update a push device registration with push-subscribe scope.
     */
    public function updateDeviceRegistration(array $device, string $deviceIdentityToken): object;

    /**
     * List push device registrations with cursor pagination.
     */
    public function listDeviceRegistrations(array $params = []): object;

    /**
     * Get a push device registration.
     */
    public function getDeviceRegistration(string $deviceId, ?string $deviceIdentityToken = null): object;

    /**
     * Delete a push device registration.
     */
    public function deleteDeviceRegistration(string $deviceId, ?string $deviceIdentityToken = null): object;

    /**
     * Delete all device registrations for a client identifier.
     */
    public function removeDeviceRegistrationsByClient(string $clientId): object;

    /**
     * Upsert a push channel subscription.
     */
    public function upsertChannelPushSubscription(array $subscription, ?string $deviceIdentityToken = null): object;

    /**
     * List push channel subscriptions with cursor pagination.
     */
    public function listChannelPushSubscriptions(array $params = [], ?string $deviceIdentityToken = null): object;

    /**
     * Delete push channel subscriptions.
     */
    public function deleteChannelPushSubscriptions(array $params, ?string $deviceIdentityToken = null): object;

    /**
     * List subscribed channels with cursor pagination.
     */
    public function listChannelPushSubscriptionChannels(array $params = []): object;

    /**
     * List stored push provider credentials with cursor pagination.
     */
    public function listPushCredentials(array $params = []): object;

    /**
     * Store or update a provider credential payload.
     */
    public function putPushCredential(string $provider, array $credential): object;

    /**
     * Publish push asynchronously by default.
     */
    public function publishPush(array $request): object;

    /**
     * Alias of publishPush.
     */
    public function publishPushDirect(array $request): object;

    /**
     * Publish a batch of push notifications asynchronously by default.
     */
    public function publishPushBatch(array $requests): object;

    /**
     * Schedule a push publish; requires notBeforeMs in the request.
     */
    public function schedulePush(array $request): object;

    /**
     * Get the status for a publish id.
     */
    public function getPublishStatus(string $publishId): object;

    /**
     * Cancel a scheduled publish.
     */
    public function cancelScheduledPush(string $publishId): object;

    /**
     * Submit a provider delivery status event.
     */
    public function postPushDeliveryStatus(array $event): object;

    /**
     * GET arbitrary REST API resource using a synchronous http client.
     * All request signing is handled automatically.
     *
     * @param string $path   Path excluding /apps/APP_ID
     * @param array  $params API params (see http://sockudo.com/docs/rest_api)
     * @param bool   $associative When true, return the response body as an associative array, else return as an object
     *
     * @throws SockudoException   Throws exception if curl wasn't initialized correctly
     * @throws ApiErrorException Throws ApiErrorException if the Channels HTTP API responds with an error
     * @throws GuzzleException
     *
     * @return mixed See Sockudo API docs
     */
    public function get(string $path, array $params = [], bool $associative = false);

    /**
     * Creates a socket signature.
     *
     * @param string $channel
     * @param string $socket_id
     * @param string|null $custom_data
     * @return string Json encoded authentication string.
     * @throws SockudoException Throws exception if $channel is invalid or above or $socket_id is invalid
     */
    public function socketAuth(string $channel, string $socket_id, ?string $custom_data = null): string;

    /**
     * Creates a presence signature (an extension of socket signing).
     *
     * @param mixed  $user_info
     *
     * @throws SockudoException Throws exception if $channel is invalid or above or $socket_id is invalid
     *
     */
    public function presenceAuth(string $channel, string $socket_id, string $user_id, $user_info = null): string;

    /**
     * Verify that a webhook actually came from Sockudo, decrypts any
     * encrypted events, and marshals them into a PHP object.
     *
     * @param array  $headers a array of headers from the request (for example, from getallheaders())
     * @param string $body    the body of the request (for example, from file_get_contents('php://input'))
     *
     * @throws SockudoException
     *
     * @return Webhook marshalled object with the properties time_ms (an int) and events (an array of event objects)
     */
    public function webhook(array $headers, string $body): object;

    /**
     * Verify that a given Sockudo Signature is valid.
     *
     * @param array  $headers an array of headers from the request (for example, from getallheaders())
     * @param string $body    the body of the request (for example, from file_get_contents('php://input'))
     *
     * @throws SockudoException if signature is incorrect.
     */
    public function verifySignature(array $headers, string $body);


    /*******************************************************************
     *
     * DEPRECATION WARNING:
     *
     * all the functions below have been deprecated in favour of their
     * camelCased variants. They will be removed in the next major
     * update.
     */

    /**
     * Get information, such as subscriber and user count, for a channel.
     *
     * @deprecated in favour of getChannelInfo
     *
     * @param string $channel The name of the channel
     * @param array  $params  Additional parameters for the query e.g. $params = array( 'info' => 'connection_count' )
     *
     * @throws SockudoException   If $channel is invalid or if curl wasn't initialized correctly
     * @throws ApiErrorException Throws ApiErrorException if the Channels HTTP API responds with an error
     * @throws GuzzleException
     *
     */
    public function get_channel_info(string $channel, array $params = []): object;

    /**
     * Fetch a list containing all channels.
     *
     * @deprecated in favour of getChannels
     *
     * @param array $params Additional parameters for the query e.g. $params = array( 'info' => 'connection_count' )
     *
     * @throws SockudoException   Throws exception if curl wasn't initialized correctly
     * @throws ApiErrorException Throws ApiErrorException if the Channels HTTP API responds with an error
     * @throws GuzzleException
     *
     */
    public function get_channels(array $params = []): object;

    /**
     * Fetch user ids currently subscribed to a presence channel.
     *
     * @deprecated in favour of getPresenceUsers
     *
     * @param string $channel The name of the channel
     *
     * @throws SockudoException   Throws exception if curl wasn't initialized correctly
     * @throws ApiErrorException Throws ApiErrorException if the Channels HTTP API responds with an error
     * @throws GuzzleException
     *
     */
    public function get_users_info(string $channel): object;

    /**
     * Fetch durable history for a channel.
     *
     * @deprecated in favour of getChannelHistory
     */
    public function get_channel_history(string $channel, array $params = []): object;

    /**
     * Creates a socket signature.
     *
     * @deprecated in favour of socketAuth
     *
     * @param string $channel
     * @param string $socket_id
     * @param string|null $custom_data
     * @return string Json encoded authentication string.
     * @throws SockudoException Throws exception if $channel is invalid or above or $socket_id is invalid
     */
    public function socket_auth(string $channel, string $socket_id, ?string $custom_data = null): string;

    /**
     * Creates a presence signature (an extension of socket signing).
     *
     * @deprecated in favour of presenceAuth
     *
     * @param mixed  $user_info
     *
     * @throws SockudoException Throws exception if $channel is invalid or above or $socket_id is invalid
     *
     */
    public function presence_auth(string $channel, string $socket_id, string $user_id, $user_info = null): string;

    /**
     * Verify that a given Sockudo Signature is valid.
     *
     * @deprecated in favour of verifySignature
     *
     * @param array  $headers an array of headers from the request (for example, from getallheaders())
     * @param string $body    the body of the request (for example, from file_get_contents('php://input'))
     *
     * @throws SockudoException if signature is incorrect.
     */
    public function ensure_valid_signature(array $headers, string $body);
}
