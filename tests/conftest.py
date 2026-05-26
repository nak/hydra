import shutil
import socket
import subprocess
import sys
import tempfile
from contextlib import closing
from pathlib import Path

import pytest


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

@pytest.fixture
def free_port():
    return find_free_port()


@pytest.fixture(scope='session')
def ca_credentials(tmp_path_factory):
    # Use Pytest's built-in session-safe temp directory allocator
    tmp_dir = tmp_path_factory.mktemp("ssl_vault")

    ca_key = tmp_dir / "ca.key"
    ca_pem = tmp_dir / "ca.pem"

    # 1. Generate local CA Root
    cmd1 = f"openssl genrsa -out {ca_key} 4096"
    subprocess.run(cmd1, shell=True, check=True)

    cmd2 = f"openssl req -x509 -new -nodes -key {ca_key} -sha256 -days 10 -out {ca_pem} -subj '/CN=MyLocalCA'"
    subprocess.run(cmd2, shell=True, check=True)

    yield ca_pem, ca_key


@pytest.fixture(scope='session')
def signed_server(ca_credentials):
    ca_pem, ca_key = ca_credentials
    base_dir = ca_pem.parent

    server_key = base_dir / "server.key"
    server_csr = base_dir / "server.csr"
    server_pem = base_dir / "server.pem"

    # 2. Generate and Sign Server credentials
    subprocess.run(f"openssl genrsa -out {server_key} 2048", shell=True, check=True)
    subprocess.run(f"openssl req -new -key {server_key} -out {server_csr} -subj '/CN=localhost'", shell=True,
                   check=True)
    subprocess.run(
        f"openssl x509 -req -in {server_csr} -CA {ca_pem} -CAkey {ca_key} -CAcreateserial -out {server_pem} -days 1 -sha256",
        shell=True, check=True)

    # CRITICAL: Use yield, not return, for proper fixture lifecycle state stability
    yield server_pem, server_key, ca_pem


@pytest.fixture(scope='session')
def signed_client(ca_credentials):
    ca_pem, ca_key = ca_credentials
    base_dir = ca_pem.parent

    client_key = base_dir / "client.key"
    client_csr = base_dir / "client.csr"
    client_pem = base_dir / "client.pem"

    # 3. Generate and Sign Client credentials (using cl.srl to avoid serial collisions)
    subprocess.run(f"openssl genrsa -out {client_key} 2048", shell=True, check=True)
    subprocess.run(f"openssl req -new -key {client_key} -out {client_csr} -subj '/CN=MyClientApp'", shell=True,
                   check=True)
    subprocess.run(
        f"openssl x509 -req -in {client_csr} -CA {ca_pem} -CAkey {ca_key} -CAserial {base_dir}/cl.srl -CAcreateserial -out {client_pem} -days 1 -sha256",
        shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    yield client_pem, client_key, ca_pem