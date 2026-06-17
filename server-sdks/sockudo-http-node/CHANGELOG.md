# Changelog

## 5.3.3

- [FIXED] Bump node-fetch to ^2.7.0, express to ^4.22.1, eslint to ^7.32.0, mocha to ^9.2.2
- [FIXED] Pin transitive dependencies via overrides: cipher-base, cookie, elliptic, form-data, lodash, pbkdf2, qs, sha.js, tar

## 5.3.2

- [Added] missing error types for Typescript support

## 5.3.1

- [CHANGED] Fix Github Actions.

## 5.2.0

- [CHANGED] Remove old notification references. It's no longer being used

## 5.1.3

[FIXED] Parsing of the extraTokens in webhook's isValid method

## 5.1.2

- [CHANGED] Add types/node-fetch to dependencies.

## 5.1.1-beta (2022-06-01)

[FIXED] Updated typescript types with new user features.

## 5.1.0-beta (2022-04-22)

[ADDED] Support for terminating user connections based on user id
[ADDED] Support for sending messages to users based on user id
[ADDED] Support for implementing user authentication endpoints
[DEPRECATED] authenticate function is deprecated. The same functionality (and interface) is now provided by authorizeChannel

## 5.0.1 (2022-01-24)

[FIXED] Incorrect `require` on `version.js` was causing a compilation error in Webpack
[FIXED] Inconsistent encoding for shared secret between other SDKs

## 5.0.0 (2021-02-18)

[BREAKING CHANGE] `trigger` now accepts a `params` _object_ instead of a `socket_id` as the third parameter.

[ADDED] Support for requesting channel attributes as part of a `trigger` and `triggerBatch` request via an `info` parameter.

## 4.0.2 (2020-11-30)

[FIXED] Workaround for a [Webpack bug](https://github.com/webpack/webpack/issues/4742) (thanks @JJ-Kidd)

## 4.0.1 (2020-11-09)

[FIXED] The type definitions had drifted out of sync with the implementation.

## 4.0.0 (2020-10-20)

[BREAKING CHANGE] Methods that previously took callbacks now return promises.

- `trigger`, `triggerBatch`, `get`, and `post` return a promise which resolves
  to a [`Response`](https://github.com/node-fetch/node-fetch#class-response) (as provided by [node-fetch](https://github.com/node-fetch/node-fetch)),
  or rejects to a `RequestError`. Of particular note:
  - `Response` has a `status` where is used to have a `statusCode`.
  - `RequestError` has a `status` where is used to have a `statusCode` to
    mirror the above.
  - `Response` has
    [`json`](https://github.com/node-fetch/node-fetch#bodyjson),
    [`text`](https://github.com/node-fetch/node-fetch#bodytext), etc to
    access the body where before it exposed `body` as a string.

[BREAKING CHANGE] The `Pusher` constructor no longer accepts `proxy` or `keepAlive`, but instead accepts [`agent`](https://nodejs.org/api/https.html#https_class_https_agent).

- To configure a proxy, use [`https://www.npmjs.com/package/https-proxy-agent`](https://www.npmjs.com/package/https-proxy-agent) or similar:

```js
const pusher = new Pusher.forURL(process.env.PUSHER_URL, {
  agent: new HttpsProxyAgent("http://localhost:8321"),
})
```

- To configure keep alive:

```js
const pusher = new Pusher.forURL(process.env.PUSHER_URL, {
  agent: new https.Agent({ keepAlive: true }),
})
```

[REMOVED] Specific Parse Cloud build. I don't believe it is required any more, but please open an issue if there are any problems using this release on Parse Cloud!

## 3.0.1 (2020-03-27)

[UPGRADED] development dependencies

[ADDED] encryptionMasterKeyBase64 constructor parameter to make it easier to use the full range of 32 byte binary values in encryption key

[DEPRECATED] encryptionMasterKey constructor parameter - use encryptionMasterKeyBase64

## 3.0.0 (2019-09-26)

[REMOVED] Support for Node versions < 8

[UPGRADED] Upgraded versions of `nock`, and `mocha`

## 2.2.2 (2019-07-31)

[FIXED] an issue where a function that generated signatures behaved differently in different versions of node. Improved the underlying compare function to unify behaviour between versions.

[ADDED] Supported platforms to the Readme and TravisCI config.

## 2.2.1 (2019-07-03)

no-op release to fix the description on <https://www.npmjs.com/package/pusher>

## 2.2.0 (2018-11-26)

[ADDED] This release adds support for end-to-end encryption, a new feature for Channels. Read more [in our docs](https://pusher.com/docs/client_api_guide/client_encrypted_channels).

[DEPRECATED] Renamed `encrypted` option to `useTLS` - `encrypted` will still work!

## 2.1.4 (2018-11-22)

[UPGRADED] Upgraded versions of `request`, `nock`, and `mocha`

## 2.1.3 (2018-08-21)

[FIXED] incorrect channels limit when triggering

## 2.1.2 (2018-07-27)

[FIXED] typescript dependency issue

## 2.1.1 (2018-07-27)

[ADDED] typescript type declarations file

## 2.0.1 (2018-07-04)

no-op release to remove some files from npm

## 2.0.0 (2018-05-08)

[FIXED] remove security vulnerability by updating requests.

[REMOVED] support for versions of node < 4

## 1.5.1 (2016-12-01)

[FIXED] fix the version of request we are using, new minor version break backwards compatibility

## 1.5.0 (2016-08-23)

[ADDED] support for publishing on up to 10 interests.

## 1.4.0 (2016-08-15)

[ADDED] support for sending push notifications.

## 1.3.0 (2016-05-24)

[ADDED] Support for triggering batch events.

## 1.2.1 (2016-03-03)

[FIXED] remove security vulnerability by updating requests.

## 1.2.0 (2016-02-16)

[ADDED] `cluster` option in the Pusher constructor.

## 1.1.0 (2016-01-05)

[ADDED] Build option for Parse Cloud.

## 1.0.6 (2015-05-18)

[FIXED] compatibility issue with node 0.12 (issue #12)

Optimisation: Use Date.now() instead of (new Date).getTime()

Run tests with Travis-CI

## 1.0.5 (2015-05-12)

[FIXED] Add validation to channel names and socket ids

## 1.0.4 (2015-02-13)

[FIXED] Auth signatures being incorrect when channel data included utf-8 characters

## 1.0.3 (2015-01-28)

[FIXED] Fixed trigger not accepting '=', '@', ',', '.' and ';' in channel names

## 1.0.2 (2014-10-23)

[FIXED] Upgraded `request` to 2.45.0 to prevent DoS vulnerability caused by `qs` dependency.

## 1.0.1 (2014-08-11)

[CHANGED] Incorrect arguments to `authenticate` will raise an error

## 1.0.0 (2014-07-14)

First stable release.
