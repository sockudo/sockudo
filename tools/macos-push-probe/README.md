# Sockudo macOS Push Probe

Small SwiftUI macOS app for testing Sockudo APNs push notifications on a Mac.

The app uses the local `client-sdks/sockudo-swift` package and its
`SockudoPushRegistration` client to:

- register the Mac with APNs
- activate the APNs token as a Sockudo push device
- subscribe the device to a push channel
- publish a test push through Sockudo

## Run

Open the project in Xcode:

```bash
open tools/macos-push-probe/SockudoMacPushProbe.xcodeproj
```

Set a signing team that can use the bundle ID, then run the
`SockudoMacPushProbe` target. The default bundle ID is:

```text
com.sockudo.macosprobe
```

The app defaults to local Sockudo credentials:

```text
Sockudo URL: http://127.0.0.1:6001
App ID: app-id
App Key: app-key
App Secret: app-secret
Channel: macos-push-probe
```

Sockudo must be running with APNs credentials whose topic matches the macOS app
bundle ID. For sandbox testing, the server APNs topic should be
`com.sockudo.macosprobe` unless you changed the app bundle ID.

## Flow

1. Click `Register With APNs` if the app has not received a token yet.
2. Click `Activate Device`.
3. Click `Subscribe Channel`.
4. Click `Publish Test`.

The app displays the APNs token, the last Sockudo API responses, and foreground
notifications it receives.
