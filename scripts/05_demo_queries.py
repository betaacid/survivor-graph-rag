import argparse
import datetime
import json
import logging
import sys
import textwrap
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

from lib.query import DEMO_QUESTIONS, query_graph_rag, query_traditional_rag


def print_divider(char="=", width=80):
    print(char * width)


def print_wrapped(text, indent=2, width=76):
    for line in text.split("\n"):
        wrapped = textwrap.fill(line, width=width, initial_indent=" " * indent, subsequent_indent=" " * indent)
        print(wrapped)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Run only first N questions (smoke test)")
    args = parser.parse_args()

    runs_dir = Path(__file__).resolve().parent.parent / "data" / "demo_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_path = runs_dir / f"{timestamp}.jsonl"
    log.info("Results will be persisted to %s", run_path)

    print_divider()
    print("  SURVIVOR: Traditional RAG vs Graph RAG — Side-by-Side Demo")
    print_divider()

    questions_run = 0
    for category in DEMO_QUESTIONS:
        print(f"\n{'':>2}--- {category['category']} ---\n")

        for question in category["questions"]:
            if args.limit is not None and questions_run >= args.limit:
                log.info("Stopping after %d questions (--limit)", args.limit)
                break
            questions_run += 1
            print_divider("-")
            print(f"  Q: {question}")
            print_divider("-")

            trad_record = {"question": question, "mode": "traditional_rag", "answer": None, "chunks": [], "time_s": None}
            graph_record = {"question": question, "mode": "graph_rag", "answer": None, "cypher": None, "graph_rows": 0, "time_s": None}

            print("\n  [Traditional RAG]")
            t0 = time.time()
            try:
                trad_answer, trad_context = query_traditional_rag(question)
                trad_time = time.time() - t0
                print(f"  Time: {trad_time:.1f}s | Chunks retrieved: {len(trad_context)}")
                print_wrapped(trad_answer)
                log.info("Traditional RAG answer for '%s': %s", question, trad_answer[:200])
                trad_record["answer"] = trad_answer
                trad_record["time_s"] = round(trad_time, 2)
                trad_record["chunks"] = [
                    {"season_title": c["season_title"], "similarity": round(c["similarity"], 4)}
                    for c in trad_context
                ]
            except Exception as e:
                log.exception("Traditional RAG failed")
                print(f"  Error: {e}")
                trad_record["answer"] = f"ERROR: {e}"

            print(f"\n  [Graph RAG]")
            t0 = time.time()
            try:
                graph_answer, cypher, graph_results = query_graph_rag(question)
                graph_time = time.time() - t0
                result_count = len(graph_results) if isinstance(graph_results, list) else 0
                print(f"  Time: {graph_time:.1f}s | Graph rows: {result_count}")
                print(f"  Cypher: {cypher[:120]}{'...' if len(cypher) > 120 else ''}")
                print_wrapped(graph_answer)
                log.info("Graph RAG cypher for '%s': %s", question, cypher)
                log.info("Graph RAG answer for '%s': %s", question, graph_answer[:200])
                graph_record["answer"] = graph_answer
                graph_record["cypher"] = cypher
                graph_record["graph_rows"] = result_count
                graph_record["time_s"] = round(graph_time, 2)
            except Exception as e:
                log.exception("Graph RAG failed")
                print(f"  Error: {e}")
                graph_record["answer"] = f"ERROR: {e}"

            with open(run_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(trad_record, ensure_ascii=False) + "\n")
                f.write(json.dumps(graph_record, ensure_ascii=False) + "\n")

            print()

        if args.limit is not None and questions_run >= args.limit:
            break

    print_divider()
    print("  Demo complete.")
    print_divider()
    log.info("All results saved to %s", run_path)


if __name__ == "__main__":
    main()
