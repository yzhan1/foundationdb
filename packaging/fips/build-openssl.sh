#!/bin/bash
# Builds static OpenSSL 3.5.9 and the OpenSSL FIPS Provider 3.1.2 (fips.so, FIPS 140-3, CMVP certificate #4985)
# into PREFIX, then writes PREFIX/ssl/openssl.cnf so binaries linked against these static libraries run in FIPS
# mode with no environment variables (libcrypto's compiled-in OPENSSLDIR and MODULESDIR point at PREFIX).
#
# Usage: packaging/fips/build-openssl.sh [PREFIX]    (default /opt/openssl-fips)
#        cmake -DOPENSSL_ROOT_DIR=PREFIX ...
# Run it in a scratch directory; tarballs already there are used instead of downloading (offline builders).
# Needs a C toolchain, make and perl with IPC::Cmd, Time::Piece and Digest::SHA
# (Rocky 9: perl-IPC-Cmd perl-Time-Piece perl-Digest-SHA).
set -euo pipefail

PREFIX=${1:-/opt/openssl-fips}
JOBS=${JOBS:-$(nproc)}

fetch() { # version sha256
	local tgz=openssl-$1.tar.gz
	[ -f "$tgz" ] || curl -fsSLO "https://github.com/openssl/openssl/releases/download/openssl-$1/$tgz"
	echo "$2  $tgz" | sha256sum -c -
	rm -rf "openssl-$1" && tar xzf "$tgz"
}

fetch 3.5.9 603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a
fetch 3.1.2 a0ce69b8b97ea6a35b96875235aa453b966ba3cba8af2de23657d8b6767d6539

(cd openssl-3.5.9 &&
	./Configure --prefix="$PREFIX" --openssldir="$PREFIX/ssl" no-shared CFLAGS="-fPIC -O3" &&
	make -j"$JOBS" && make install_sw install_ssldirs)
(cd openssl-3.1.2 &&
	./Configure enable-fips --prefix="$PREFIX" --openssldir="$PREFIX/ssl" &&
	make -j"$JOBS" && make install_fips)

# fips.so 3.1.2 also offers X25519/X448, which #4985 does not approve, so TLS groups are limited to NIST curves.
cat >"$PREFIX/ssl/openssl.cnf" <<EOF
config_diagnostics = 1
openssl_conf = openssl_init
.include $PREFIX/ssl/fipsmodule.cnf
[openssl_init]
providers = provider_sect
alg_section = algorithm_sect
ssl_conf = ssl_sect
[provider_sect]
fips = fips_sect
base = base_sect
[base_sect]
activate = 1
[algorithm_sect]
default_properties = fips=yes
[ssl_sect]
system_default = system_default_sect
[system_default_sect]
Groups = P-256:P-384:P-521
EOF

env -u OPENSSL_CONF -u OPENSSL_MODULES "$PREFIX/bin/openssl" list -providers
