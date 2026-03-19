"""
Example 03 -- ClickHouse Module

Demonstrates the testcontainers *module* API:
  - ClickHouseContainer handles image, ports, env vars, and wait logic
  - get_connection_url() gives a ready-to-use connection string
  - Compare how little code is needed vs. examples 01 and 02

This is how you'd use testcontainers in real test suites.
"""

import clickhouse_driver
from testcontainers.clickhouse import ClickHouseContainer


def run() -> None:
    print("\n=== Example 03: ClickHouse Module ===\n")

    # ---- 1. One line to create a fully configured container ------------------
    print("[1] Creating ClickHouseContainer (module handles everything)...")

    with ClickHouseContainer("clickhouse/clickhouse-server:latest") as clickhouse:
        url = clickhouse.get_connection_url()
        print(f"[2] Container ready. Connection URL: {url}")

        # ---- 2. Connect ------------------------------------------------------
        client = clickhouse_driver.Client.from_url(url)

        # ---- 3. Create a table -----------------------------------------------
        print("[3] Creating table 'users'...")
        client.execute("""
            CREATE TABLE users (
                id    UInt32,
                name  String,
                email String
            ) ENGINE = MergeTree()
            ORDER BY id
        """)

        # ---- 4. Insert data ---------------------------------------------------
        print("[4] Inserting 3 rows...")
        client.execute(
            "INSERT INTO users (id, name, email) VALUES",
            [
                (1, "Alice", "alice@example.com"),
                (2, "Bob", "bob@example.com"),
                (3, "Charlie", "charlie@example.com"),
            ],
        )

        # ---- 5. Query it back -------------------------------------------------
        rows = client.execute("SELECT id, name, email FROM users ORDER BY id")
        print("[5] Query results:")
        for row in rows:
            print(f"    id={row[0]}, name={row[1]}, email={row[2]}")

        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        print("[6] All assertions passed!")

    # Container is stopped and removed automatically
    print("[7] Container torn down. Done!\n")


if __name__ == "__main__":
    run()
