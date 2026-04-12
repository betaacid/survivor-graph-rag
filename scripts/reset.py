import logging

from dotenv import load_dotenv

from lib.neo4j_client import clear_graph
from lib.pg_client import setup_schema, truncate_chunks

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def main():
    log.info("Resetting databases for fresh run...")
    setup_schema()
    truncate_chunks()
    log.info("  Postgres: chunks table truncated")
    clear_graph()
    log.info("  Neo4j: graph cleared")
    log.info("Done. Ready for a fresh pipeline run.")


if __name__ == "__main__":
    main()
