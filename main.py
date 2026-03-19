"""
Testcontainers Presentation -- Example Runner

Run with: uv run main.py
"""

import subprocess
import sys

EXAMPLES = {
    "1": ("Basic Container (DockerContainer + manual setup)", "example_01_basic"),
    "2": ("Wait Strategies (HttpWait, LogWait, Composite)", "example_02_wait_strategies"),
    "3": ("ClickHouse Module (high-level API)", "example_03_module"),
    "4": ("Network Isolation (two containers talking)", "example_04_network"),
    "5": ("Pytest + ClickHouse Module (run via pytest)", "example_05_pytest"),
    "6": ("Pytest: Partitioning, ORDER BY, TTL (run via pytest)", "example_06_pytest_advanced"),
}


def show_menu() -> None:
    print()
    print("=" * 55)
    print("  Testcontainers Presentation")
    print("=" * 55)
    print()
    for key, (title, _) in EXAMPLES.items():
        print(f"  [{key}] {title}")
    print()
    print("  [a] Run all examples")
    print("  [q] Quit")
    print()


def run_example(module_name: str) -> None:
    if module_name in ("example_05_pytest", "example_06_pytest_advanced"):
        print(f"\n  Running: pytest {module_name}.py -v\n")
        subprocess.run([sys.executable, "-m", "pytest", f"{module_name}.py", "-v"])
    else:
        module = __import__(module_name)
        module.run()


def main() -> None:
    while True:
        show_menu()
        choice = input("Pick an example: ").strip().lower()

        if choice == "q":
            print("Bye!")
            sys.exit(0)

        if choice == "a":
            for _, module_name in EXAMPLES.values():
                run_example(module_name)
            continue

        if choice in EXAMPLES:
            _, module_name = EXAMPLES[choice]
            run_example(module_name)
        else:
            print(f"Unknown option: {choice}")


if __name__ == "__main__":
    main()
