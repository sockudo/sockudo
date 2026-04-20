# Ably Push Notifications Research

## Scope

This research covers Ably Push Notifications using:

- `https://ably.com/docs/push`
- `https://ably.com/docs/push/configure/device`
- `https://ably.com/docs/push/configure/web`
- `https://ably.com/docs/push/publish`
- `https://ably.com/docs/api/realtime-sdk/push-admin`
- `https://ably.com/docs/platform/account/app/notifications`

It fully addresses:

- overview
- getting started
- Web Push
- APNs
- FCM
- configuration and activation
- devices
- browsers
- publishing

## Overview

Ably positions push notifications as offline-capable delivery to:

- devices through FCM or APNs
- browsers through Web Push

Key product properties:

- devices/browsers do not need an active Ably connection to receive push notifications
- operating systems and browsers maintain their own battery-efficient push transport
- direct publishing and channel-based publishing are both supported
- push subscriptions do not count against connection limits
- channel-based push still activates channels, which affects peak concurrent channel count

## Delivery models

Ably supports two major delivery modes.

### Direct publishing

Targets are selected explicitly by:

- `deviceId`
- `clientId`
- recipient attributes such as transport type and device token

Direct publishing is used for:

- personal alerts
- targeted account notifications
- per-device messages

### Channel-based publishing

Messages are published to an Ably channel with a push payload in `extras`.

All push-subscribed devices/browsers on that channel receive the notification.

This is used for:

- topic or room notifications
- fan-out to groups
- alert broadcast by channel membership

## Push notification process

From Ably's overview, the lifecycle is:

1. configure the third-party push service credentials
2. activate a device or browser directly or via a server
3. subscribe devices/browsers to channels if using channel-based push
4. publish push messages directly or through channels
5. Ably packages and forwards the notification to FCM, APNs, or Web Push
6. the platform-specific service delivers to the end device/browser

## Configuration and activation

## FCM configuration

Ably's documented FCM setup:

1. create/manage the Firebase project
2. create service account keys
3. download the service account JSON
4. upload it in the Ably dashboard notifications setup

This is Ably's Android push foundation.

## APNs configuration

Ably's notifications settings page adds APNs detail:

1. create an Apple push certificate in the Apple Developer Program
2. export it as `.p12`
3. either:
   - import the `.p12` file directly into Ably's dashboard
   - or convert it to PEM using OpenSSL and paste certificate/private-key contents into the dashboard

Ably even documents the OpenSSL conversion command from `.p12` to `.pem`.

Important parity implication:

- Sockudo needs a serious operator-facing credential-management surface, not just an API endpoint

## Web Push configuration

Ably's browser configuration page requires:

- VAPID key generation
- service worker hosting
- `push` event handling in the service worker
- browser-side push activation through the Ably push plugin

Ably notes:

- the first activation of a web browser generates a VAPID key pair
- that key pair is then reused for the application

The service worker is a first-class integration requirement because Web Push is service-worker mediated.

## Devices versus browsers

Ably treats both as push targets, but the activation paths differ.

### Devices

- use FCM or APNs
- SDK registers with the platform push service
- Ably activation registers the device with Ably and stores a `deviceIdentityToken`

### Browsers

- use Web Push
- service worker registration and browser push manager are involved
- Ably push plugin is required in browser JavaScript integrations

## Activation modes

Ably supports two activation patterns for both devices and browsers.

### Direct activation

Typical steps:

1. authenticate the Ably client
2. generate/store a unique local device/browser identifier
3. register with the underlying push platform and obtain its token/identifier
4. register with Ably using platform details plus the recipient details
5. store the returned identity token for future authenticated updates

Important behavior:

- calling activate again is a no-op if already activated
- registration persists after app/browser close until deactivated

### Server-assisted activation

This pattern is for reducing client privileges.

The client:

- still registers with the platform push provider
- sends the resulting token/identifier to the app server

The server:

- registers or deregisters the device/browser with Ably through the Push Admin API
- stores and updates tokens when they rotate

This matters for Sockudo because a serious push implementation needs:

- low-trust client support
- privileged server orchestration

## Device activation specifics

Ably's device page includes Android server-assisted details:

- `push.activate(context, true)` and `push.deactivate(context, true)` with custom registration enabled
- broadcast intents for register/deregister
- callbacks that return `deviceIdentityToken`

It also documents a device activation lifecycle with broadcast events for:

- activation
- deactivation
- registration failure

## Browser activation specifics

Ably's browser page includes:

- package-manager setup with the push plugin
- CDN setup with `AblyPushPlugin`
- direct activation with `client.push.activate()`
- server-assisted activation via `registerCallback` and `deregisterCallback`
- handling failed updates via `updateFailedCallback`

## Getting started

Ably's push docs distribute "getting started" across:

- browser/device configuration pages
- platform-specific SDK guides
- dashboard setup
- publish examples

The docs also show APNs getting started guidance for Swift/iOS in search results, indicating Ably expects complete platform tutorials rather than just raw APIs.

## Web Push details

Ably's documented Web Push specifics include:

- service worker requirement
- push event listener example
- VAPID key lifecycle
- Ably JS push plugin
- support for browser activation and deactivation callbacks

This is not optional frosting. Web Push parity requires:

- service worker integration guidance
- VAPID management
- browser registration and update handling

## APNs details

From the payload and configuration docs:

- APNs credentials may be supplied through imported `.p12` or PEM material
- APNs-specific payload overrides are supported under `apns`
- background notifications require `apns-headers`
- Ably documents `apns-push-type` and `apns-priority` for background delivery

This means Ably does not flatten APNs to a generic subset. It exposes APNs-specific control when needed.

## FCM details

Ably's Android/FCM integration includes:

- Firebase service account JSON uploaded to Ably
- FCM-specific payload overrides under `fcm`
- direct publish targeting by device or client

Again, Ably exposes provider-specific override space, not only a generic abstraction.

## Publishing

## Generic payload structure

Ably defines a push payload structure with:

- `notification`
- `data`
- provider-specific override objects such as:
  - `apns`
  - `fcm`
  - `web`

### Generic-to-provider mapping

Ably documents mapping across FCM, APNs, and Web Push for:

- `notification.title`
- `notification.body`
- `notification.icon`
- `notification.sound`
- `notification.collapseKey`
- `data`

Important nuance:

- some generic fields are discarded on some platforms
- some are remapped to platform-native equivalents
- `data` becomes root-level on APNs but `notification.data` on Web Push

Sockudo parity therefore needs a payload transformation layer, not pass-through JSON only.

### Provider overrides

Ably allows:

- overriding generic values for one provider
- adding provider-specific fields not modeled in the generic structure

Examples documented:

- custom APNs alert title
- custom FCM notification color
- custom Web Push badge

## Direct publish

Ably supports direct publish using:

- `deviceId`
- `clientId`
- recipient attributes

Examples show:

- per-device publish
- per-client publish to all devices associated with that client
- direct publish by recipient attributes such as:
  - `transportType: 'apns'`
  - `deviceToken`

Direct publish is also available in batch mode.

## Batch publish

Ably documents a batch push endpoint:

- `POST /push/batch/publish`

Use case:

- large numbers of distinct notifications to multiple recipients in one request

Ably also advises:

- if the same notification should go to many recipients, prefer channel-based push

## Publish via channels

To publish through channels, Ably requires:

1. push enabled on the channel or namespace via rules
2. clients with `push-subscribe`
3. a valid push payload placed in the message `extras.push` field

Devices/browsers only receive the channel-published push if:

- the push rule is enabled
- the device/browser is subscribed for push on that channel
- the payload is compatible with the target platform

Ably distinguishes channel push subscriptions from ordinary message subscriptions.

## Channel subscription APIs

Ably exposes per-channel push helpers:

- `subscribeDevice()`
- `subscribeClient()`
- `unsubscribeDevice()`
- `unsubscribeClient()`
- `listSubscriptions()`

This is an important design signal:

- push subscription state is a first-class surface attached to channels

## Push Admin API

Ably's Push Admin API exists for backend management and is available on realtime and REST SDKs via `client.push.admin`.

Required capabilities:

- `push-admin` for full management
- `push-subscribe` for restricted self-management as a push target

Authentication options documented for push admin flows:

- Ably token containing the target device/browser `deviceId`
- standard key/token with `deviceIdentityToken` in request header

## Device registrations API

Ably documents:

- `get`
- `list`
- `save`
- `remove`
- `removeWhere`

Notable behaviors:

- `list` supports filtering by `clientId`, `deviceId`, and `state`
- device states are `ACTIVE`, `FAILING`, `FAILED`
- deleting a non-existent device is treated as success

## Channel subscriptions admin API

Ably documents:

- `list`
- `listChannels`
- `save`
- `remove`
- `removeWhere`

Filters include:

- `channel`
- `clientId`
- `deviceId`

## DeviceDetails model

Ably documents a `DeviceDetails` shape including:

- `id`
- `clientId`
- `formFactor`
- `metadata`
- `platform`
- `deviceSecret`
- `push.recipient`
- `push.state`
- `push.error`

This matters because parity is not only message delivery. It includes operator-visible registry state and failure state.

## Error handling and diagnostics

Ably's overview page documents a push metachannel:

- `[meta]log:push`

This channel carries push events and errors not otherwise available to clients.

Operational implication:

- parity should include meta-log or inspection surfaces for push failures

## Dashboard tooling

Ably's notifications settings page also documents dashboard tooling:

- push inspector for manual testing
- inspection of channel subscriptions
- inspection of device registrations
- inspection of client registrations

This is operationally important and should be part of Sockudo's long-term parity plan.

## Design conclusions relevant to Sockudo

To reach parity in behavior and capability, Sockudo needs:

- provider credential management for FCM, APNs, and Web Push
- device and browser activation workflows
- direct and server-assisted registration
- direct publish, batch publish, and channel-based publish
- provider-specific payload transformation
- channel-scoped push subscription model
- admin registry APIs for devices and subscriptions
- per-device state and failure tracking
- operator inspection and error channels

If Sockudo only implements "publish JSON to FCM", it will not be close to Ably parity.

## Source links

- [Push notifications overview](https://ably.com/docs/push)
- [Configure and activate devices](https://ably.com/docs/push/configure/device)
- [Configure and activate web browsers](https://ably.com/docs/push/configure/web)
- [Publish and receive push notifications](https://ably.com/docs/push/publish)
- [Push Notifications - Admin API](https://ably.com/docs/api/realtime-sdk/push-admin)
- [Notifications settings](https://ably.com/docs/platform/account/app/notifications)
