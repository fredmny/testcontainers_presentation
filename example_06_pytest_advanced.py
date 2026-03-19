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

from datetime import date, datetime, timedelta
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
    TTL event_date + INTERVAL 30 DAY MOVE TO VOLUME 'warm',
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
        # ClickHouse stores both TTL rules: the VOLUME move and the
        # retention-period rule.  The DELETE keyword is implicit (it's
        # the default TTL action) so ClickHouse normalizes it away.
        assert "TO VOLUME 'warm'" in create_stmt
        assert "toIntervalDay(365)" in create_stmt


class TestPartitioning:
    """Verify data is split into the correct partitions."""

    def test_monthly_partitions(self, client: clickhouse_driver.Client) -> None:
        """Inserting data across months should create separate partitions."""
        client.execute(CREATE_EVENTS_TABLE)

        # Use recent dates so the TTL rules (30-day move, 365-day delete)
        # don't expire the data before we can query it.
        today = date.today()
        month1 = today.replace(day=15)
        month2 = (today.replace(day=1) - timedelta(days=1)).replace(day=10)  # prev month
        month3 = (month2.replace(day=1) - timedelta(days=1)).replace(day=5)  # 2 months ago

        client.execute(
            "INSERT INTO events (event_date, event_time, user_id, event_type, payload) VALUES",
            [
                (month1, datetime.combine(month1, datetime.min.time()), 1, "click", "m1"),
                (month2, datetime.combine(month2, datetime.min.time()), 2, "view", "m2"),
                (month3, datetime.combine(month3, datetime.min.time()), 1, "click", "m3"),
            ],
        )

        rows = client.execute("""
            SELECT DISTINCT partition
            FROM system.parts
            WHERE database = 'test' AND table = 'events' AND active = 1
            ORDER BY partition
        """)
        partitions = [row[0] for row in rows]
        expected = sorted({
            month1.strftime("%Y%m"),
            month2.strftime("%Y%m"),
            month3.strftime("%Y%m"),
        })
        for p in expected:
            assert p in partitions, f"Expected partition {p} in {partitions}"

    def test_order_by_within_partition(self, client: clickhouse_driver.Client) -> None:
        """Data should be sorted by (user_id, event_time) within a partition."""
        client.execute(CREATE_EVENTS_TABLE)

        # Use today's date (well within TTL) and insert into a single month.
        today = date.today()
        client.execute(
            "INSERT INTO events (event_date, event_time, user_id, event_type, payload) VALUES",
            [
                (today, datetime.combine(today, datetime.min.time().replace(hour=15)), 3, "click", "c"),
                (today, datetime.combine(today, datetime.min.time().replace(hour=10)), 1, "view", "a"),
                (today, datetime.combine(today, datetime.min.time().replace(hour=12)), 1, "click", "b"),
            ],
        )

        this_month = int(today.strftime("%Y%m"))
        rows = client.execute(f"""
            SELECT user_id, event_time, payload
            FROM events
            WHERE toYYYYMM(event_date) = {this_month}
            ORDER BY user_id, event_time
        """)
        # user 1 first (sorted by event_time), then user 3
        assert len(rows) >= 3
        # Find the rows we just inserted by payload
        our_rows = [r for r in rows if r[2] in ("a", "b", "c")]
        assert our_rows[0][0] == 1  # user_id=1, 10:00
        assert our_rows[1][0] == 1  # user_id=1, 12:00
        assert our_rows[2][0] == 3  # user_id=3, 15:00
