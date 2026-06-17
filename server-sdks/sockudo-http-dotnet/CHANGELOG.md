# Changelog

## 5.0.0
* [ADDED] Add support for channel attributes when triggering events

## 4.7.0-beta.2
* [ADDED] Constructor for PusherRestClient accepting an externally supplied HttpClient

## 4.7.0-beta
* [ADDED] AuthenticateUser
* [ADDED] AuthorizeChannel
* [DEPRECATED] Authenticate
* [CHANGED] Bump Newtonsoft.Json to version 13.0.2

## 4.6.1
* [FIXED] PusherClient test integration to 2.1.0.

## 4.6.0
* [ADDED] Strong name to the PusherServer assembly.

## 4.5.0
* [ADDED] End-to-end encryption using NaCl.Net.
* [ADDED] EncryptionMasterKey and RestClientTimeout properties to IPusherOptions.
* [ADDED] Cluster property to test application settings.
* [CHANGED] Default timeout from 100 to 30 seconds.

## 4.4.0
* [REMOVED] PusherServer.Core project
* [FIXED] Use .ConfigureAwait(false) on every await

## 4.3.2
* [CHANGED] Make the GH release task depend tag creation

## 4.3.1
* [CHANGED] PusherServer.Core should not be packed

## 4.3.0
* [CHANGED] PusherServer and PusherServer.Core project structure to target net45, net472, netstandard1.3 and netstandard2.0
* [FIXED] TriggerResult exception text should include content
* [CHANGED] Bump NUnit version to 3.11
* [FIXED] Failing tests for Pusher apps in a cluster other than the default
* [ADDED] Json config file for test application settings
* [ADDED] Exception classes EventBatchSizeExceededException and EventDataSizeExceededException for client side validation
* [ADDED] BatchEventDataSizeLimit to IPusherOptions
* [CHANGED] Default Json serializer to use the option NullValueHandling.Ignore
* [ADDED] Exception classes ChannelNameFormatException, ChannelNameLengthExceededException and SocketIdFormatException
* [ADDED] Interface ITraceLogger and applied it as a property to IPusherOptions

## 4.2.0
* [CHANGED] Project now targets DotNet Standard 1.6. The API has not changed.

## 4.0.0-rc1, 4.1.0
* [ADDED] support for the .NET Core
* [CHANGED] The library now is built against .NET 4.5
* [CHANGED] Removed the dependency on RestSharp, and replaced with the .NETs HttpClient class.
* [REMOVED] Retired the callback syntax
* [ADDED] Async/Await based syntax
* [FIXED] The Out of range exceptions thrown by Trigger methods to correctly specify the message, instead of the message be supplied as the parameter name

## 3.0.0 rc4

* [ADDED] Support for the `cluster` option.

## 3.0.0 rc3

* [ADDED] Build support for the Pusher Travis instance
* [CHANGED] Trigger based calls that result in a bad response no longer throw TriggerResponseExceptions. The response and original content are now available as properties for interrogation
* [ADDED] The Pusher HTTP API Host & Port can now be set via PusherOptions.HostName & PusherOptions.Port properties
* [ADDED] TriggerAsync to allow asynchronous requests to be made to the HTTP API.
* [ADDED] New API abstractions onto Pusher for fetching info on single and multiple channels, and fetching users from a presence Channel. Both sync and sync calls are supported.
* [REMOVED] Event Buffer support
* [ADDED] Support for providing a different JSON serializer & deserializer

## 3.0.0 rc1/2

* [CHANGED] `Trigger` calls that result in a non 200 response from the Pusher HTTP API now result in a `TriggerResponseException` being thrown.
			This is a BREAKING CHANGE as previously you could inspect the `ITriggerResult.StatusCode` to detect a failed request.
* [ADDED]	When triggering events against a Pusher cluster that supports Event Buffer functionality the IDs of the triggered events
			can be retrieved via `ITriggerResult.EventIds`
* [ADDED]	The Pusher HTTP API Host can now be set via `PusherOptions.Host`
* [ADDED]	`TriggerAsync` to allow asynchronous requests to be made to the HTTP API.

## 2.1.1

* [FIXED] Channel name and socket_id values are validated for `Pusher.Trigger`
* [FIXED] socket_id values are validated for `Pusher.Authenticate`

## 2.1.0

* [FIXED] Pusher.Authenticate for private channels. Should not return `channel_data` in the JSON.
* [CHANGED] IAuthenticationData Pusher.Authenticate(string channelName, string socketId, PresenceChannelData presenceData) to throw a ArgumentNullException if `presenceData` is `null`.

## 2.0.0

* Full release of 2.0.0 library
* Added `Pusher.ProcessWebHook(signature, body)` support

## 2.0.0-beta-5

* Updating RestSharp to 105.0.1
