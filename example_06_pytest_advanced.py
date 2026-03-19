"""
Example 06 -- Pytest: Partitioning, ORDER BY, and TTL with Volume Moves

Tests a ClickHouse table that uses:
  - PARTITION BY toYYYYMM(event_date)  -- monthly partitions
  - ORDER BY (user_id, event_time)     -- sparse index for fast lookups
  - TTL with TO VOLUME 'warm'          -- tiered storage (move old data)
  - TTL with DELETE                    -- drop data after retention period

This example also shows how to customize the container beyond what the
module gives you out of the box: we mount a custom storage policy XML
config so the container has a "tiered" policy with hot + warm volumes.

Run with:  uv run pytest example_06_pytest_advanced.py -v
"""

from datetime import date, datetime
from pathlib import Path

import clickhouse_driver
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:latest"

# Path to the custom storage policy config (mounted into the container)
STORAGE_CONFIG = str(Path(__file__).parent / "config" / "storage.xml")

CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS events (
        event_date  Date,
        event_time  DateTime,
        user_id     UInt64,
        event_type  String,
        payload     String
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (user_id, event_time)
    TTL event_date + INTERVAL 30 DAY TO VOLUME 'warm',
        event_date + INTERVAL 365 DAY DELETE
    SETTINGS storage_policy = 'tiered'
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def clickhouse_container():
    """
    Start a ClickHouse container with a custom storage policy.

    We use DockerContainer instead of ClickHouseContainer here because
    we need to mount a config file and create the warm storage directory
    before ClickHouse starts. This shows that you can always drop down
    to the generic API when the module doesn't cover your use case.
    """
    # The warm disk path (/var/lib/clickhouse_warm) must exist BEFORE
    # ClickHouse starts, or the server will refuse to boot.  We override
    # the entrypoint with a small shell wrapper that creates the directory
    # and then execs the normal entrypoint.  This is a handy pattern any
    # time you need to do pre-boot setup inside a testcontainer.
    startup_cmd = (
        "mkdir -p /var/lib/clickhouse_warm"
        " && chown clickhouse:clickhouse /var/lib/clickhouse_warm"
        " && exec /entrypoint.sh"
    )

    container = (
        DockerContainer(CLICKHOUSE_IMAGE)
        .with_exposed_ports(9000, 8123)
        .with_env("CLICKHOUSE_DB", "test")
        .with_env("CLICKHOUSE_USER", "test")
        .with_env("CLICKHOUSE_PASSWORD", "test")
        .with_volume_mapping(
            STORAGE_CONFIG,
            "/etc/clickhouse-server/config.d/storage.xml",
            "ro",
        )
        .with_kwargs(entrypoint=["/bin/bash", "-c", startup_cmd])
        .waiting_for(
            HttpWaitStrategy(8123, "/ping")
            .for_status_code(200)
            .with_startup_timeout(30)
        )
    )

    with container:
        yield container


@pytest.fixture()
def client(clickhouse_container) -> clickhouse_driver.Client:
    """Provide a connected ClickHouse client."""
    host = clickhouse_container.get_container_host_ip()
    port = int(clickhouse_container.get_exposed_port(9000))
    return clickhouse_driver.Client(
        host=host, port=port, user="test", password="test", database="test"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestStoragePolicy:
    """Verify the custom storage policy was loaded."""

    def test_tiered_policy_exists(self, client: clickhouse_driver.Client) -> None:
        """The 'tiered' policy should be available with hot + warm volumes."""
        rows = client.execute("""
            SELECT volume_name
            FROM system.storage_policies
            WHERE policy_name = 'tiered'
            ORDER BY volume_priority
        """)
        volume_names = [row[0] for row in rows]
        assert "hot" in volume_names
        assert "warm" in volume_names

    def test_warm_disk_exists(self, client: clickhouse_driver.Client) -> None:
        """The 'warm_disk' should be registered."""
        rows = client.execute("""
            SELECT name FROM system.disks WHERE name = 'warm_disk'
        """)
        assert len(rows) == 1


class TestTableCreation:
    """Verify the table is created with correct partitioning, ordering, and TTL."""

    def test_create_events_table(self, client: clickhouse_driver.Client) -> None:
        """CREATE TABLE should succeed with the tiered storage policy."""
        client.execute(CREATE_EVENTS_TABLE)

        # Verify the table exists
        rows = client.execute("""
            SELECT name
            FROM system.tables
            WHERE database = 'test' AND name = 'events'
        """)
        assert rows == [("events",)]

    def test_partition_key(self, client: clickhouse_driver.Client) -> None:
        """Partition key should be toYYYYMM(event_date)."""
        client.execute(CREATE_EVENTS_TABLE)

        rows = client.execute("""
            SELECT partition_key
            FROM system.tables
            WHERE database = 'test' AND name = 'events'
        """)
        assert rows[0][0] == "toYYYYMM(event_date)"

    def test_sorting_key(self, client: clickhouse_driver.Client) -> None:
        """Sorting (ORDER BY) key should be (user_id, event_time)."""
        client.execute(CREATE_EVENTS_TABLE)

        rows = client.execute("""
            SELECT sorting_key
            FROM system.tables
            WHERE database = 'test' AND name = 'events'
        """)
        assert rows[0][0] == "user_id, event_time"

    def test_storage_policy(self, client: clickhouse_driver.Client) -> None:
        """Table should use the 'tiered' storage policy."""
        client.execute(CREATE_EVENTS_TABLE)

        rows = client.execute("""
            SELECT storage_policy
            FROM system.tables
            WHERE database = 'test' AND name = 'events'
        """)
        assert rows[0][0] == "tiered"

    def test_ttl_rules_in_create_statement(self, client: clickhouse_driver.Client) -> None:
        """The CREATE TABLE statement should contain both TTL rules."""
        client.execute(CREATE_EVENTS_TABLE)

        rows = client.execute("""
            SELECT create_table_query
            FROM system.tables
            WHERE database = 'test' AND name = 'events'
        """)
        create_stmt = rows[0][0]
        assert "TO VOLUME 'warm'" in create_stmt
        assert "DELETE" in create_stmt


class TestPartitioning:
    """Verify data is split into the correct partitions."""

    def test_monthly_partitions(self, client: clickhouse_driver.Client) -> None:
        """Inserting data across months should create separate partitions."""
        client.execute(CREATE_EVENTS_TABLE)

        client.execute(
            "INSERT INTO events (event_date, event_time, user_id, event_type, payload) VALUES",
            [
                (date(2024, 1, 15), datetime(2024, 1, 15, 10, 0, 0), 1, "click", "jan"),
                (date(2024, 2, 10), datetime(2024, 2, 10, 12, 0, 0), 2, "view", "feb"),
                (date(2024, 3, 5), datetime(2024, 3, 5, 14, 0, 0), 1, "click", "mar"),
            ],
        )

        rows = client.execute("""
            SELECT DISTINCT partition
            FROM system.parts
            WHERE database = 'test' AND table = 'events' AND active = 1
            ORDER BY partition
        """)
        partitions = [row[0] for row in rows]
        assert "202401" in partitions
        assert "202402" in partitions
        assert "202403" in partitions

    def test_order_by_within_partition(self, client: clickhouse_driver.Client) -> None:
        """Data should be sorted by (user_id, event_time) within a partition."""
        client.execute(CREATE_EVENTS_TABLE)

        # Insert multiple rows in the same month, different users/times
        client.execute(
            "INSERT INTO events (event_date, event_time, user_id, event_type, payload) VALUES",
            [
                (date(2024, 6, 1), datetime(2024, 6, 1, 15, 0, 0), 3, "click", "c"),
                (date(2024, 6, 1), datetime(2024, 6, 1, 10, 0, 0), 1, "view", "a"),
                (date(2024, 6, 1), datetime(2024, 6, 1, 12, 0, 0), 1, "click", "b"),
            ],
        )

        rows = client.execute("""
            SELECT user_id, event_time, payload
            FROM events
            WHERE toYYYYMM(event_date) = 202406
            ORDER BY user_id, event_time
        """)
        # user 1 first (sorted by event_time), then user 3
        assert rows[0][0] == 1  # user_id=1, 10:00
        assert rows[1][0] == 1  # user_id=1, 12:00
        assert rows[2][0] == 3  # user_id=3, 15:00
