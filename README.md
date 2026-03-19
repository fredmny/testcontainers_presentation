# Testcontainers -- Presentation Script

## What is Testcontainers?

- A library that lets you spin up **real Docker containers** from your code (or tests)
- Containers are created on demand, used, and **automatically destroyed** when done
- Available for Python, Java, Go, .NET, Node, Rust, and more
- Provides **modules** (pre-configured containers) for popular services: databases, message brokers, cloud SDKs, etc.

## What problem does it solve?

- **"Works on my machine"** -- everyone gets an identical, fresh container every run
- **No shared staging databases** -- no more tests stepping on each other's data, no state drift between runs
- **No manual setup** -- no `docker-compose up` before running tests, no remembering to tear down after
- **No port conflicts** -- testcontainers maps to random available host ports automatically
- **No leftover state** -- each test run starts with a clean container, no data from previous runs leaking in
- **CI-friendly** -- same code works locally and in CI, no special Docker config needed

## Why not just use regular containers (docker-compose, scripts, etc.)?

| Regular containers                           | Testcontainers                                       |
| -------------------------------------------- | ---------------------------------------------------- |
| Manual start/stop (or scripts to manage)     | Lifecycle tied to test/code scope (context manager)   |
| Fixed ports -- can conflict in CI            | Random port mapping -- no conflicts                   |
| Shared state between test runs               | Fresh container every time                            |
| Forgotten containers waste resources         | Ryuk sidecar kills orphans automatically              |
| Config lives in separate files (compose.yml) | Config lives next to the test that needs it           |
| Hard to customize per-test                   | Each test can configure its own container differently |

## Key concepts

- **DockerContainer** -- the generic, low-level API. You choose the image, ports, env vars, and wait logic yourself.
- **Wait strategies** -- tell testcontainers how to know the container is actually ready (log message, HTTP endpoint, TCP port, etc.)
- **Modules** -- pre-built wrappers (e.g. `ClickHouseContainer`) that handle image, ports, env, and readiness for you
- **Network** -- create isolated Docker networks so multiple containers can discover each other by alias
- **Context manager** -- always use `with container:` so cleanup happens even if your code crashes
- **Ryuk** -- a sidecar container that testcontainers starts automatically to garbage-collect any containers you forgot to stop

## Examples in this repo

| #   | File                            | What it shows                                                          |
| --- | ------------------------------- | ---------------------------------------------------------------------- |
| 1   | `example_01_basic.py`           | Generic `DockerContainer`, manual ports/env, HTTP poll for readiness   |
| 2   | `example_02_wait_strategies.py` | `HttpWaitStrategy`, `PortWaitStrategy`, `CompositeWaitStrategy`        |
| 3   | `example_03_module.py`          | `ClickHouseContainer` module -- all boilerplate handled for you        |
| 4   | `example_04_network.py`         | Two containers on a shared `Network`, querying each other by alias     |
| 5   | `example_05_pytest.py`          | Example 03 rewritten as pytest -- fixtures, assertions, test structure |
| 6   | `example_06_pytest_advanced.py` | Pytest: partitioning, ORDER BY, TTL with volume moves, custom config   |
| -   | `test_clickhouse.py`            | Full pytest test suite -- how you'd use testcontainers in a real project |

## How to run

```bash
# Install dependencies
uv sync

# Run the interactive menu
uv run main.py

# Or run individual examples directly
uv run example_01_basic.py
uv run example_02_wait_strategies.py
uv run example_03_module.py
uv run example_04_network.py

# Run example 05 (pytest)
uv run pytest example_05_pytest.py -v

# Run example 06 (pytest -- advanced ClickHouse features)
uv run pytest example_06_pytest_advanced.py -v

# Run the full pytest test suite
uv run pytest test_clickhouse.py -v
```

## GitHub Actions (CI)

See `.github/workflows/clickhouse-tests.yml` -- a minimal workflow that runs the pytest suite on every push/PR.

The key takeaway: **the workflow has zero Docker/ClickHouse configuration**. No `services:` block, no `docker-compose`, no port mappings. Testcontainers handles all of that inside the test code. The workflow just runs `pytest`.

GitHub-hosted runners (`ubuntu-latest`) come with Docker pre-installed, so testcontainers works out of the box.

## Lazydocker demo tip

- Open a **second terminal** and run `lazydocker`
- Then run example 02 from the first terminal -- it pauses for 10 seconds after the container is ready
- Watch the container appear in lazydocker, then disappear when the example finishes
- This makes the lifecycle management very visible to the audience
- You'll see **two containers**: your ClickHouse container and `testcontainers-ryuk` -- that's the cleanup sidecar. If your process crashes or gets killed before teardown, Ryuk removes orphaned containers automatically. It's the safety net that makes testcontainers reliable in CI.

## Takeaways

- Use **modules** when one exists for your service -- they save a lot of boilerplate
- Use **wait strategies** so your code doesn't try to connect before the service is ready
- The `with` block (context manager) is the safest pattern -- cleanup happens automatically
- Testcontainers is not just for tests -- it's useful for demos, local dev, and prototyping too
- The Ryuk sidecar is your safety net -- even if your code crashes, containers get cleaned up
