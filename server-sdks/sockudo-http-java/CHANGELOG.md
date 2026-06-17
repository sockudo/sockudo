# Changelog

## 1.3.5

- [CHANGED] Update async-http-client to 3.0.6.
- [CHANGED] Change dependency scopes to api.
- [CHANGED] Upgrade Netty to 4.1.129.Final.

## 1.3.4

- [CHANGED] Updated org.asynchttpclient:async-http-client to 3.0.1 to address a vulnerability
- [CHANGED] Updated CI matrix to remove Java 8 and add Java 21, reflecting current support policy
- [CHANGED] Replaced deprecated  constructor with  in 
- [REMOVED] Deprecated org.ajoberstar.github-pages plugin and associated configuration

## 1.3.1 2022-05-16

- [CHANGED] Use SecureRandom.nextBytes instead of SecureRandom.generateSeed

## 1.3.0 2021-09-02

- [ADDED] Add end-to-end encryption support

## 1.2.1 2021-04-19

- [CHANGED] Upgraded asynchttpclient to v2.12.13 (https://nvd.nist.gov/vuln/detail/CVE-2019-20444)

## 1.2.0 2020-04-20

- [ADDED] Support for asynchronous http calls
