import platform
import socket
import subprocess
from contextlib import closing
from pathlib import Path

import pytest
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))  # Ensure src/ is in the path for imports
from hydra.nano_services.http import WebApplication

if platform.system() == 'Windows':
    openssl = 'openssl.exe'
else:
    openssl = 'openssl'


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
    args1 = f"genrsa -out {ca_key} 4096".split()
    subprocess.run([openssl] + args1, shell=False, check=True)

    args2 = f"req -x509 -new -nodes -key {ca_key} -sha256 -days 1 -out {ca_pem} -subj /CN=MyLocalCA".split()
    subprocess.run([openssl] + args2, shell=False, check=True)

    yield ca_pem, ca_key


@pytest.fixture(scope='session')
def signed_server(ca_credentials):
    ca_pem, ca_key = ca_credentials
    base_dir = ca_pem.parent

    server_key = base_dir / "server.key"
    server_csr = base_dir / "server.csr"
    server_pem = base_dir / "server.pem"

    # 2. Generate and Sign Server credentials
    subprocess.run([openssl] + f"genrsa -out {server_key} 2048".split(), shell=False, check=True)
    assert server_key.exists(), f"Expected server key file to be created at {server_key}, but it does not exist."
    subprocess.run([openssl] + f"req -new -key {server_key} -out {server_csr} -subj /CN=localhost".split(), shell=False,
                   check=True)
    subprocess.run([openssl] +
        f"x509 -req -in {server_csr} -CA {ca_pem} -CAkey {ca_key} -CAcreateserial -out {server_pem} -days 1 -sha256".split(),
        shell=False, check=True)

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
    subprocess.run([openssl] + f"genrsa -out {client_key} 2048".split(), shell=False, check=True)
    subprocess.run([openssl] + f"req -new -key {client_key} -out {client_csr} -subj /CN=MyClientApp".split(), shell=False,
                   check=True)
    subprocess.run([openssl] +
        f"x509 -req -in {client_csr} -CA {ca_pem} -CAkey {ca_key} -CAserial {base_dir}/cl.srl -CAcreateserial -out {client_pem} -days 1 -sha256".split(),
        shell=False, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    yield client_pem, client_key, ca_pem




@pytest.fixture()
def clear_web_app():
    WebApplication._context= {}
    WebApplication._class_instance_methods = {}
    WebApplication._instance_methods_class_map= {}
    WebApplication._instance_methods = []
    WebApplication._all_methods = []
    WebApplication.routes_get = {}
    WebApplication.routes_post = {}
    WebApplication.callables_get = {}
    WebApplication.callables_post = {}
    WebApplication.module_mapping_get = {}
    WebApplication.module_mapping_post = {}
    yield
