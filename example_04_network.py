"""
Example 04 -- Network Isolation

Demonstrates Docker network support in testcontainers:
  - Create a Network so containers can discover each other by alias
  - Start two ClickHouse containers on the same network
  - Query from one container to the other using the network alias
    (via ClickHouse's remoteSecure/remote table function)

This pattern is useful for testing microservices that need to talk
to each other, or for testing distributed database setups.
"""

import clickhouse_driver
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.core.network import Network


def run() -> None:
    print("\n=== Example 04: Network Isolation ===\n")

    # ---- 1. Create a shared Docker network -----------------------------------
    print("[1] Creating a Docker network...")

    with Network() as network:
        print(f"[2] Network created: {network.name}")

        # ---- 2. Start two ClickHouse containers on the same network ----------
        print("[3] Starting ClickHouse container A (alias: 'clickhouse-a')...")
        container_a = (
            ClickHouseContainer("clickhouse/clickhouse-server:latest")
            .with_network(network)
            .with_network_aliases("clickhouse-a")
        )

        print("[4] Starting ClickHouse container B (alias: 'clickhouse-b')...")
        container_b = (
            ClickHouseContainer("clickhouse/clickhouse-server:latest")
            .with_network(network)
            .with_network_aliases("clickhouse-b")
        )

        with container_a, container_b:
            # ---- 3. Connect to each container from the host ------------------
            url_a = container_a.get_connection_url()
            url_b = container_b.get_connection_url()
            print(f"[5] Container A URL: {url_a}")
            print(f"[6] Container B URL: {url_b}")

            client_a = clickhouse_driver.Client.from_url(url_a)
            client_b = clickhouse_driver.Client.from_url(url_b)

            # ---- 4. Create a table on container B and insert data ------------
            print("[7] Creating table 'messages' on container B...")
            client_b.execute("""
                CREATE TABLE messages (
                    id      UInt32,
                    content String
                ) ENGINE = MergeTree()
                ORDER BY id
            """)
            client_b.execute(
                "INSERT INTO messages (id, content) VALUES",
                [
                    (1, "Hello from container B!"),
                    (2, "Containers can talk to each other!"),
                ],
            )

            # ---- 5. Query container B FROM container A using remote() --------
            #  The remote() table function lets ClickHouse query another
            #  ClickHouse instance by host:port. Because both containers
            #  share a network, container A can reach B via 'clickhouse-b:9000'.
            print("[8] Querying container B FROM container A via network alias...")
            rows = client_a.execute("""
                SELECT id, content
                FROM remote('clickhouse-b:9000', 'test', 'messages', 'test', 'test')
                ORDER BY id
            """)

            print("[9] Results (fetched from B through A):")
            for row in rows:
                print(f"    id={row[0]}, content={row[1]}")

            assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
            print("[10] Cross-container query succeeded!")

        print("[11] Both containers torn down.")
    print("[12] Network removed. Done!\n")


if __name__ == "__main__":
    run()
