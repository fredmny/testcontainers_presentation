"""
Tests using ClickHouseContainer with pytest.

This is how you'd use testcontainers in a real test suite:
  - A session-scoped fixture spins up ClickHouse once for all tests
  - Each test gets a fresh client and can create/query tables
  - The container is torn down automatically when the test session ends

Run with:  uv run pytest test_clickhouse.py -v
"""

from datetime import date

import clickhouse_driver
import pytest
from testcontainers.clickhouse import ClickHouseContainer

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:latest"


@pytest.fixture(scope="session")
def clickhouse_container():
    """Start a ClickHouse container once for the entire test session."""
    with ClickHouseContainer(CLICKHOUSE_IMAGE) as container:
        yield container


@pytest.fixture()
def clickhouse_client(clickhouse_container: ClickHouseContainer) -> clickhouse_driver.Client:
    """Return a fresh ClickHouse client connected to the test container."""
    return clickhouse_driver.Client.from_url(
        clickhouse_container.get_connection_url()
    )


class TestClickHouseConnection:
    """Basic connectivity tests."""

    def test_server_is_reachable(self, clickhouse_client: clickhouse_driver.Client) -> None:
        result = clickhouse_client.execute("SELECT 1")
        assert result == [(1,)]

    def test_server_version(self, clickhouse_client: clickhouse_driver.Client) -> None:
        rows = clickhouse_client.execute("SELECT version()")
        version = rows[0][0]
        assert isinstance(version, str)
        assert len(version) > 0


class TestClickHouseCRUD:
    """Create, insert, and query operations."""

    def test_create_table_and_insert(self, clickhouse_client: clickhouse_driver.Client) -> None:
        clickhouse_client.execute("""
            CREATE TABLE IF NOT EXISTS test_users (
                id    UInt32,
                name  String,
                email String
            ) ENGINE = MergeTree()
            ORDER BY id
        """)

        clickhouse_client.execute(
            "INSERT INTO test_users (id, name, email) VALUES",
            [
                (1, "Alice", "alice@example.com"),
                (2, "Bob", "bob@example.com"),
                (3, "Charlie", "charlie@example.com"),
            ],
        )

        rows = clickhouse_client.execute(
            "SELECT id, name, email FROM test_users ORDER BY id"
        )
        assert len(rows) == 3
        assert rows[0] == (1, "Alice", "alice@example.com")
        assert rows[1] == (2, "Bob", "bob@example.com")
        assert rows[2] == (3, "Charlie", "charlie@example.com")

    def test_aggregation_query(self, clickhouse_client: clickhouse_driver.Client) -> None:
        clickhouse_client.execute("""
            CREATE TABLE IF NOT EXISTS test_events (
                event_date Date,
                user_id    UInt32,
                event_type String,
                value      Float64
            ) ENGINE = MergeTree()
            ORDER BY (event_date, user_id)
        """)

        clickhouse_client.execute(
            "INSERT INTO test_events (event_date, user_id, event_type, value) VALUES",
            [
                (date(2024, 1, 1), 1, "click", 1.0),
                (date(2024, 1, 1), 1, "click", 2.0),
                (date(2024, 1, 1), 2, "view", 1.0),
                (date(2024, 1, 2), 1, "click", 3.0),
            ],
        )

        rows = clickhouse_client.execute("""
            SELECT
                user_id,
                count()      AS event_count,
                sum(value)   AS total_value
            FROM test_events
            GROUP BY user_id
            ORDER BY user_id
        """)

        assert len(rows) == 2
        # user 1: 3 events, total 6.0
        assert rows[0] == (1, 3, 6.0)
        # user 2: 1 event, total 1.0
        assert rows[1] == (2, 1, 1.0)
