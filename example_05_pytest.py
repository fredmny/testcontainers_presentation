"""
Example 05 -- Pytest + ClickHouse Module

This is example 03 rewritten as a pytest test suite. Compare the two
side by side to see how testcontainers fits naturally into pytest:

  - Fixtures manage the container lifecycle (start on setup, stop on teardown)
  - Each test gets a connected client -- no boilerplate per test
  - Assertions are just normal pytest asserts
  - The container is shared across all tests (session scope) for speed

Run with:  uv run pytest example_05_pytest.py -v
"""

import clickhouse_driver
import pytest
from testcontainers.clickhouse import ClickHouseContainer

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:latest"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def clickhouse_container():
    """Spin up a ClickHouse container once for the whole test session."""
    with ClickHouseContainer(CLICKHOUSE_IMAGE) as container:
        yield container


@pytest.fixture()
def client(clickhouse_container: ClickHouseContainer) -> clickhouse_driver.Client:
    """Provide a fresh client connected to the session container."""
    return clickhouse_driver.Client.from_url(
        clickhouse_container.get_connection_url()
    )


# ---------------------------------------------------------------------------
# Tests -- same operations as example 03
# ---------------------------------------------------------------------------

def test_container_is_reachable(client: clickhouse_driver.Client) -> None:
    """Sanity check: the container is up and responds to queries."""
    result = client.execute("SELECT 1 AS healthy")
    assert result == [(1,)]


def test_create_table(client: clickhouse_driver.Client) -> None:
    """Create the 'users' table (same as example 03 step 3)."""
    client.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id    UInt32,
            name  String,
            email String
        ) ENGINE = MergeTree()
        ORDER BY id
    """)

    # Verify the table exists by querying system.tables
    tables = client.execute(
        "SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'users'"
    )
    assert tables == [("users",)]


def test_insert_and_query(client: clickhouse_driver.Client) -> None:
    """Insert rows and query them back (same as example 03 steps 4-5)."""
    client.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id    UInt32,
            name  String,
            email String
        ) ENGINE = MergeTree()
        ORDER BY id
    """)

    client.execute(
        "INSERT INTO users (id, name, email) VALUES",
        [
            (1, "Alice", "alice@example.com"),
            (2, "Bob", "bob@example.com"),
            (3, "Charlie", "charlie@example.com"),
        ],
    )

    rows = client.execute("SELECT id, name, email FROM users ORDER BY id")

    assert len(rows) == 3
    assert rows[0] == (1, "Alice", "alice@example.com")
    assert rows[1] == (2, "Bob", "bob@example.com")
    assert rows[2] == (3, "Charlie", "charlie@example.com")
