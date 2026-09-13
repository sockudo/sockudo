#!/usr/bin/env bash
# Generates a private CA, a leaf certificate for the mock APNs host, and an APNs-style
# ES256 provider key pair. Everything lands in ./certs and is ignored by git.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p certs
cd certs

if [[ ! -f ca.pem ]]; then
  openssl ecparam -name prime256v1 -genkey -noout -out ca-key.pem
  openssl req -x509 -new -key ca-key.pem -sha256 -days 30 -subj "/CN=Sockudo Mock APNs CA" -out ca.pem
fi

if [[ ! -f server.pem ]]; then
  openssl ecparam -name prime256v1 -genkey -noout -out server-key.pem
  openssl req -new -key server-key.pem -subj "/CN=mock-apns" -out server.csr
  cat > san.cnf <<CNF
subjectAltName=DNS:localhost,DNS:mock-apns,DNS:host.docker.internal,IP:127.0.0.1
extendedKeyUsage=serverAuth
CNF
  openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -days 30 -sha256 \
    -extfile san.cnf -out server.pem
  rm -f server.csr san.cnf
fi

if [[ ! -f AuthKey_MOCKKEY001.p8 ]]; then
  openssl ecparam -name prime256v1 -genkey -noout \
    | openssl pkcs8 -topk8 -nocrypt -out AuthKey_MOCKKEY001.p8
  openssl ec -in AuthKey_MOCKKEY001.p8 -pubout -out apns-provider-public.pem 2>/dev/null
fi
echo "certificates ready in $(pwd)"
