# Changelog

## 5.1.1

- [CHANGED] readme example for user authentication

## 5.1.0

* [ADDED] SendToUser method
* [ADDED] AuthenticateUser method
* [ADDED] AuthorizePrivateChannel method
* [ADDED] AuthorizePresenceChannel method
* [CHANGED] AuthenticatePrivateChannel method deprecated
* [CHANGED] AuthenticatePresenceChannel method deprecated

## 5.0.0 / 2021-02-19

* Breaking change: `TriggerBatch` now returns `(*TriggerBatchChannelsList, error)` instead of `error`
* Breaking change: `Channels` takes `ChannelsParams` as a parameter instead of `map[string]string`
* Breaking change: `Channel` takes `ChannelParams` as a parameter instead of `map[string]string`
* Breaking change: switches to go modules using option 1. described in https://github.com/golang/go/wiki/Modules#releasing-modules-v2-or-higher - this will cause problems for legacy package managers like `dep`
* Added `TriggerWithParams` and `TriggerMultiWithParams` - they provide support for requesting channel attributes by specifying an `Info` field
* Added a `Info` field to the `Event` type passed to `TriggerBatch`
* Deprecated `TriggerExclusive` and `TriggerMultiExclusive` (use `TriggerWithParams` and `TriggerMultiWithParams` instead)

## 4.0.4 / 2020-09-02

* Allow message size to be overridden for dedicate cluster customers (PR [#63](https://github.com/pusher/pusher-http-go/pull/71))

## 4.0.3 / 2020-07-28

* Added library name and version in HTTP Header (PR [#62](https://github.com/pusher/pusher-http-go/pull/62))
* Changed: allow larger (10KB -> 20KB) requests as we sometimes do on dedicated clusters (PR [#66](https://github.com/pusher/pusher-http-go/pull/66))

## 4.0.2 / 2020-07-28

* Added `go.mod` for managing the library as a Go module
* Changed `github.com/stretchr/testify/assert` with a stable `gopkg.in/stretchr/testify.v1/assert`

## 4.0.1 / 2020-04-01


* Added EncryptionMasterKeyBase64 parameter
* Deprecated EncryptionMasterKey parameter

## 4.0.0 / 2019-05-31

* This release modifies the entire repo to respect the go linter. This is a significant API breaking change and will likely require you to correct references to the names that were changed in your code. All future releases will respect the linter. A summary of the changes:
* Rename AppId > AppID
* Rename UserId > UserID
* Rename SocketId > SocketID
* Rename Id > ID
* Rename HttpClient > HTTPClient
* Improved comments and tabbing

## 3.0.0 / 2019-05-31

* This release removes the `*BufferedEvents` return from calls to `trigger` is it never did anything. Our documentation elsewhere conflicted with this, and it made the library more complex than it needed to be, so we removed it.

## 2.0.0 / 2019-05-31

* This release removes support for Push Notifications. Check out https://pusher.com/beams for our new, improved Push Notification offering!

## 1.3.0 / 2018-08-13

* This release adds support for end to end encrypted channels, a new feature for Channels. Read more [in our docs](https://pusher.com/docs/client_api_guide/client_encrypted_channels).

## 1.2.0 / 2016-05-24

* Add support for batch events

## 1.1.0 / 2016-02-22

* Introduce a `Cluster` option for the Pusher initializer.

## 1.0.0 / 2015-05-14

* Users can pass in a `http.Client` instance to the Pusher initializer. They can configure this instance directly to have specific options e.g. timeouts.
* Therefore, the `Timeout` field on `pusher.Client` is deprecated.
* `HttpClient()` function is no longer public. HTTP Client configuration is now done on the `HttpClient` **property** of `pusher.Client`. Read [here](https://github.com/pusher/pusher-http-go#request-timeouts) for more details.
* If no `HttpClient` is specified, the library creates one with a default timeout of 5 seconds.
* The library is now GAE compatible. Read [here](https://github.com/pusher/pusher-http-go#google-app-engine) for more details.

## 0.2.2 / 2015-05-12

* Socket_ids are now validated upon Trigger*Exclusive and channel authentication.

## 0.2.1 / 2015-04-30

* Webhook validation uses hmac.Equals to guard against timing attacks.

## 0.2.0 / 2015-03-30

* A HTTP client is shared between requests to allow configuration. If none is set by the user, the library supplies a default. Allows for pipelining or to change the transport.

## 0.1.0 / 2015-03-26

* Instantiation of client from credentials, URL or environment variables.
* User can trigger Pusher events on single channels, multiple channels, and exclude recipients
* Authentication of private and presence channels
* Pusher webhook validation
* Querying application state
* Cluster configuration, HTTPS support, timeout configuration.
