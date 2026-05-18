# Sockudo APNs E2E Probe

This harness creates a tiny iOS app that registers for APNs, posts its sandbox
device token to a local bridge, and has the bridge publish one Sockudo APNs
canary notification.

The default bundle ID is also the APNs topic:

```bash
com.sockudo.apnsprobe
```

Override it with `BUNDLE_ID=...` if Apple Developer automatic signing requires
a different explicit App ID. The APNs key team, signing team, bundle ID, and
`PUSH_APNS_TOPIC` must all match.

## Run

Start Sockudo with the APNs sandbox credentials:

```bash
BUNDLE_ID=com.sockudo.apnsprobe tools/apns-e2e/start-sockudo-apns.sh
```

In another terminal, start the token bridge:

```bash
node tools/apns-e2e/apns-bridge.mjs
```

Build, install, and launch the iOS probe on the connected iPhone:

```bash
BUNDLE_ID=com.sockudo.apnsprobe tools/apns-e2e/build-run-ios.sh
```

The app prints and displays the APNs token. The bridge writes:

- `.apns-e2e/latest-token.txt`
- `.apns-e2e/latest-summary.json`

If signing fails, the usual cause is that the Xcode account cannot create or
use the explicit App ID under team `69QGT49729`.
