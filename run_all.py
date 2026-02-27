import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent
RESET_SCRIPT = ROOT / "scripts" / "00_reset_databases.py"
SCRIPTS = [
    ROOT / "scripts" / "01_download_seasons.py",
    ROOT / "scripts" / "02_extract_tables.py",
    ROOT / "scripts" / "03_setup_traditional_rag.py",
    ROOT / "scripts" / "04_setup_graph_rag.py",
    ROOT / "scripts" / "05_demo_queries.py",
]

REQUIRED_ENV = ["OPENAI_API_KEY", "NEO4J_URI", "NEO4J_PASSWORD", "DATABASE_URL"]


def check_env():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        print(f"Missing environment variables: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in the values.")
        sys.exit(1)


def wait_for_services():
    import socket
    from urllib.parse import urlparse

    neo4j_host, neo4j_port = "localhost", 7687
    pg_host, pg_port = "localhost", 5432
    db_url = os.getenv("DATABASE_URL", "")
    if db_url:
        parsed = urlparse(db_url)
        pg_host = parsed.hostname or "localhost"
        pg_port = parsed.port or 5432

    for name, host, port in [("Neo4j", neo4j_host, neo4j_port), ("Postgres", pg_host, pg_port)]:
        for attempt in range(30):
            try:
                with socket.create_connection((host, port), timeout=2):
                    print(f"  {name} is ready")
                    break
            except OSError:
                if attempt == 0:
                    print(f"  Waiting for {name} on {host}:{port}...")
                time.sleep(2)
        else:
            print(f"  {name} not reachable after 60s. Is Docker running?")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Run the Survivor Graph RAG vs Traditional RAG pipeline")
    parser.add_argument("--reset", action="store_true", help="Reset Neo4j and Postgres before running (clears graph and chunks)")
    args = parser.parse_args()

    print("=" * 60)
    print("Survivor: Graph RAG vs Traditional RAG — Full Pipeline")
    print("=" * 60)

    step = 1
    total_steps = 7 + (1 if args.reset else 0)

    print(f"\n[{step}/{total_steps}] Checking environment...")
    check_env()
    print("  All environment variables set.")
    step += 1

    print(f"\n[{step}/{total_steps}] Checking database services...")
    wait_for_services()
    step += 1

    if args.reset:
        print(f"\n[{step}/{total_steps}] Resetting databases...")
        result = subprocess.run([sys.executable, str(RESET_SCRIPT)], cwd=ROOT)
        if result.returncode != 0:
            print(f"\n  {RESET_SCRIPT.name} failed with exit code {result.returncode}")
            sys.exit(1)
        step += 1

    for script in SCRIPTS:
        print(f"\n[{step}/{total_steps}] Running {script.name}...")
        result = subprocess.run([sys.executable, str(script)], cwd=ROOT)
        if result.returncode != 0:
            print(f"\n  {script.name} failed with exit code {result.returncode}")
            sys.exit(1)
        step += 1

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
