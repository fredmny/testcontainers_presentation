"""
Example 01 -- Basic Container

Demonstrates the lowest-level testcontainers API:
  - Use DockerContainer with a generic ClickHouse image
  - Manually expose ports and set environment variables
  - Poll the HTTP interface to know when ClickHouse is ready
  - Connect with clickhouse-driver and run a simple query

This is verbose on purpose so you can see every moving part.
"""

import time
from urllib.error import URLError
from urllib.request import urlopen

import clickhouse_driver
from testcontainers.core.container import DockerContainer

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:latest"


def wait_for_http(host: str, port: int, timeout: int = 30) -> None:
    """Poll ClickHouse's HTTP interface until it responds 'Ok.'."""
    url = f"http://{host}:{port}/ping"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except (URLError, OSError, ConnectionError):
            pass
        time.sleep(1)
    raise TimeoutError(f"ClickHouse HTTP interface at {url} not ready after {timeout}s")


def run() -> None:
    print("\n=== Example 01: Basic Container ===\n")

    # ---- 1. Configure the container manually --------------------------------
    print("[1] Creating a generic DockerContainer with the ClickHouse image...")
    container = (
        DockerContainer(CLICKHOUSE_IMAGE)
        .with_exposed_ports(9000, 8123)  # native + HTTP interfaces
        .with_env("CLICKHOUSE_DB", "demo")
        .with_env("CLICKHOUSE_USER", "demo")
        .with_env("CLICKHOUSE_PASSWORD", "demo")
    )
    breakpoint()
    # ---- 2. Start and wait ---------------------------------------------------
    with container:
        host = container.get_container_host_ip()
        native_port = int(container.get_exposed_port(9000))
        http_port = int(container.get_exposed_port(8123))

        print("[2] Container started. Polling HTTP interface until ready...")
        wait_for_http(host, http_port)
        print(f"[3] ClickHouse is ready at {host}:{native_port}")

        # ---- 3. Connect and query --------------------------------------------
        print("[4] Connecting with clickhouse-driver...")
        client = clickhouse_driver.Client(
            host=host,
            port=native_port,
            user="demo",
            password="demo",
            database="demo",
        )

        result = client.execute("SELECT 1 AS healthy")
        print(f"[5] Query result: {result}")
        assert result == [(1,)], f"Unexpected result: {result}"
        print("[6] Query succeeded!")

    # Context manager exits here -> container is stopped and removed
    print("[7] Container torn down. Done!\n")


if __name__ == "__main__":
    run()
