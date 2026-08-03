#!/bin/zsh
# Build the gated prototype from the plaintext source.
#
# Stamps a fresh version onto every data_*.js script tag before encrypting, so a
# browser can never pair new page code with a stale cached data file — that
# silently renders empty tables. Then wraps the page in the password gate.
#
# Usage: ./build.sh            (rebuilds prototype.html)
set -e
cd "$(dirname "$0")"

STAMP=$(date +%s)
TMP=$(mktemp -t protobuild).html
sed "s/BUILDSTAMP/${STAMP}/g" .plain/prototype.html > "$TMP"
node encrypt_gate.mjs "$TMP" prototype.html
rm -f "$TMP"
echo "built prototype.html with data version ${STAMP}"
