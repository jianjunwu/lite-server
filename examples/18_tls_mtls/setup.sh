#!/usr/bin/env bash
# Generate a local CA + server + client certificate for the TLS/mTLS example.
# Idempotent: skips generation once certs/ exists (the server picks up file
# changes live, so re-running this script is itself a rotation demo).
#
# Uses the two-step req -> x509 -req flow everywhere (with -extfile) so it
# works on both OpenSSL and LibreSSL. The CA carries basicConstraints/keyUsage
# so strict TLS stacks (Python ssl, rustls) accept it; the server cert carries
# serverAuth EKU and the client cert clientAuth EKU.
set -euo pipefail
cd "$(dirname "$0")"

if [ -f certs/server.crt ] && [ -f certs/client.crt ]; then
    echo "certs/ already exists — skipping generation"
    exit 0
fi

mkdir -p certs

CA_EXT="basicConstraints=critical,CA:true
keyUsage=critical,keyCertSign,cRLSign"
SERVER_EXT="subjectAltName=DNS:localhost,IP:127.0.0.1
basicConstraints=critical,CA:false
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth"
CLIENT_EXT="basicConstraints=critical,CA:false
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth"

# 1. Local CA (self-signed via x509 -req -signkey)
openssl req -newkey rsa:2048 -keyout certs/ca.key -out certs/ca.csr \
    -nodes -subj "/CN=lite-server-test-ca"
openssl x509 -req -in certs/ca.csr -signkey certs/ca.key -out certs/ca.crt \
    -days 3650 -extfile <(printf "%s\n" "$CA_EXT")

# 2. Server certificate (SAN: localhost + 127.0.0.1, serverAuth EKU)
openssl req -newkey rsa:2048 -keyout certs/server.key -out certs/server.csr \
    -nodes -subj "/CN=localhost"
openssl x509 -req -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key \
    -CAcreateserial -out certs/server.crt -days 3650 \
    -extfile <(printf "%s\n" "$SERVER_EXT")

# 3. Client certificate (signed by the same CA — required by mTLS; clientAuth EKU)
openssl req -newkey rsa:2048 -keyout certs/client.key -out certs/client.csr \
    -nodes -subj "/CN=test-client"
openssl x509 -req -in certs/client.csr -CA certs/ca.crt -CAkey certs/ca.key \
    -CAcreateserial -out certs/client.crt -days 3650 \
    -extfile <(printf "%s\n" "$CLIENT_EXT")

rm -f certs/*.csr
echo "certificates generated in certs/"
