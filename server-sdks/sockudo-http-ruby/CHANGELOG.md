# Changelog

## 2.0.4

* [ADDED] Add `authenticate_user` method for user authentication flow

## 2.0.3

* [FIXED] Corrected the channels limit when publishing events. Upped from 10 to 100. 

## 2.0.2

* [CHANGED] made encryption_master_key_base64 globally configurable 

## 2.0.1

* [CHANGED] Only include lib and essential docs in gem.

## 2.0.0

* [CHANGED] Use TLS by default.
* [REMOVED] Support for Ruby 2.4 and 2.5.
* [FIXED] Handle empty or nil  configuration.
* [REMOVED] Legacy Push Notification integration.
* [ADDED] Stalebot and Github actions. 

## 1.4.3

  * [FIXED] Remove newline from end of base64 encoded strings, some decoders don't like
    them.

## 1.4.2
==================

  * [FIXED] Return `shared_secret` to support authenticating encrypted channels. Thanks
    @Benjaminpjacobs

## 1.4.1

  * [CHANGED] Remove rbnacl from dependencies so we don't get errors when it isn't
    required. Thanks @y-yagi!

## 1.4.0

  * [ADDED] Support for end-to-end encryption.

## 1.3.3

  * [CHANGED] Rewording to clarify "Pusher Channels" or simply "Channels" product name.

## 1.3.2

  * [FIXED] Return a specific error for "Request Entity Too Large" (body over 10KB).
  * [ADDED] Add a `use_tls` option for SSL (defaults to false).
  * [ADDED] Add a `from_url` client method (in addition to existing `from_env` option).
  * [CHANGED] Improved documentation and fixed typos.
  * [ADDED] Add Ruby 2.4 to test matrix.

## 1.3.1

  * [FIXED] Added missing client batch methods to default client delegations
  * [CHANGED] Document raised exception in the `authenticate` method
  * [FIXED] Fixes em-http-request from using v2.5.0 of `addressable` breaking builds.

## 1.3.0

  * [ADDED] Add support for sending push notifications on up to 10 interests.

## 1.2.1

  * [FIXED] Fixes Rails 5 compatibility. Use duck-typing to detect request object

## 1.2.0

  * [CHANGED] Minor release for Native notifications

## 1.2.0.rc1

  * [ADDED] Add support for Native notifications

## 1.1.0

  * [ADDED] Add support for batch events

## 1.0.0

 * [CHANGED] No breaking changes, this release is just to follow semver and show that we
are stable.

## 0.18.0

  * [ADDED] Introduce `Pusher::Client.from_env`
  * [FIXED] Improve error handling on missing config

## 0.17.0

  * [ADDED] Introduce the `cluster` option.

## 0.16.0

  * [CHANGED] Bump httpclient version to 2.7
  * [REMOVED] Ruby 1.8.7 is not supported anymore.

## 0.15.2

  * [CHANGED] Documented `Pusher.channel_info`, `Pusher.channels`
  * [ADDED] Added `Pusher.channel_users`

## 0.15.1

  * [FIXED] Fixed a bug where the `authenticate` method added in 0.15.0 wasn't exposed on the Pusher class.

## 0.15.0

  * [ADDED] Added `Pusher.authenticate` method for authenticating private and presence channels.
    This is prefered over the older `Pusher['a_channel'].authenticate(...)` style.

## 0.14.6

  * [CHANGED] Updated to use the `pusher-signature` gem instead of `signature`.
    This resolves namespace related issues.

## 0.14.5

  * [SECURITY] Prevent auth delegation trough crafted socket IDs

## 0.14.4

  * [SECURITY] Prevent timing attack, update signature to v0.1.8
  * [SECURITY] Prevent POODLE. Disable SSLv3, update httpclient to v2.5
  * [FIXED] Fix channel name character limit.
  * [ADDED] Adds support for listing users on a presence channel

## 0.14.2

  * [CHANGED] Bump httpclient to v2.4. See #62 (POODLE SSL)
  * [CHANGED] Fix limited channel count at README.md. Thanks @tricknotes
