#!/usr/bin/env python3
"""Run the net I/O offload unit tests (noSim/flow/Net2/NetIO/) with TLS.

unit/flow_test/native gives flow_test no TLS configuration, so these tests skip their TLS cases there. This generates
a certificate chain with mkcert and runs them with it, through the FDB_TLS_* environment variables.
"""

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mkcert", required=True, help="path to the mkcert tool")
    parser.add_argument("--flow-test", required=True, help="path to flow_test")
    parser.add_argument("--seed", default="1", help="flow_test --seed")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="netio_tls_test.") as directory:
        root = Path(directory)
        files = {
            name: root / "{}.pem".format(name)
            for name in (
                "server_cert",
                "server_key",
                "server_ca",
                "client_cert",
                "client_key",
                "client_ca",
            )
        }
        command = [
            args.mkcert,
            "--server-chain-length",
            "3",
            "--client-chain-length",
            "3",
        ]
        for name, path in files.items():
            command += ["--{}-file".format(name.replace("_", "-")), str(path)]
        subprocess.run(
            command, check=True, cwd=root, stdout=subprocess.DEVNULL, timeout=120
        )
        for path in files.values():
            if not path.is_file():
                sys.exit("mkcert did not write {}".format(path))

        environment = dict(
            os.environ,
            FDB_TLS_CERTIFICATE_FILE=str(files["server_cert"]),
            FDB_TLS_KEY_FILE=str(files["server_key"]),
            FDB_TLS_CA_FILE=str(files["server_ca"]),
        )
        result = subprocess.run(
            [args.flow_test, "--seed", args.seed, "--filter", "noSim/flow/Net2/NetIO/"],
            env=environment,
        )
        return result.returncode


if __name__ == "__main__":
    sys.exit(main())
