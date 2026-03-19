"""
Example 02 -- Wait Strategies

Demonstrates testcontainers wait strategies:
  - HttpWaitStrategy:  waits for ClickHouse HTTP /ping endpoint to return 200
  - PortWaitStrategy:  waits for the native protocol port (9000) to accept TCP
  - CompositeWaitStrategy: combines multiple strategies (run sequentially)

After the container is ready we pause for 10 seconds so you can
inspect it with lazydocker (or `docker ps`) before teardown.
"""

import time

import clickhouse_driver
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import (
    CompositeWaitStrategy,
    HttpWaitStrategy,
    PortWaitStrategy,
)

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:latest"


def run() -> None:
    print("\n=== Example 02: Wait Strategies ===\n")

    # ---- 1. Build the wait strategy -----------------------------------------
    # We combine two strategies (executed sequentially):
    #   a) Wait for the HTTP /ping endpoint (port 8123) to respond 200
    #   b) Wait for the native protocol port (9000) to accept TCP connections
    wait = CompositeWaitStrategy(
        HttpWaitStrategy(8123, "/ping").for_status_code(200).with_startup_timeout(30),
        PortWaitStrategy(9000).with_startup_timeout(30),
    )

    print("[1] Creating container with composite wait strategy...")
    print("    - HttpWaitStrategy(port=8123, path='/ping', status=200)")
    print("    - PortWaitStrategy(port=9000)")

    container = (
        DockerContainer(CLICKHOUSE_IMAGE)
        .with_exposed_ports(9000, 8123)
        .with_env("CLICKHOUSE_DB", "demo")
        .with_env("CLICKHOUSE_USER", "demo")
        .with_env("CLICKHOUSE_PASSWORD", "demo")
        .waiting_for(wait)
    )

    # ---- 2. Start -- wait strategies run automatically on .start() ----------
    with container:
        host = container.get_container_host_ip()
        native_port = int(container.get_exposed_port(9000))
        http_port = int(container.get_exposed_port(8123))

        print(f"[2] Container is ready!")
        print(f"    Native protocol : {host}:{native_port}")
        print(f"    HTTP interface  : {host}:{http_port}")

        # ---- 3. Pause for lazydocker demo -----------------------------------
        print()
        print("    ==========================================")
        print("    Container is RUNNING -- check lazydocker!")
        print("    Waiting 10 seconds before teardown...")
        print("    ==========================================")

        for remaining in range(10, 0, -1):
            print(f"    Tearing down in {remaining}s...", end="\r")
            time.sleep(1)
        print()

        # ---- 4. Quick query to prove it works --------------------------------
        print("[3] Running a quick query before teardown...")
        client = clickhouse_driver.Client(
            host=host,
            port=native_port,
            user="demo",
            password="demo",
            database="demo",
        )
        result = client.execute("SELECT 'wait strategies work!' AS message")
        print(f"[4] Query result: {result}")

    print("[5] Container torn down. Done!\n")


if __name__ == "__main__":
    run()
