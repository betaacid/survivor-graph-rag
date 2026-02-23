import json
import logging
import re
from io import StringIO
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

MAX_TABLE_HTML_KB = 30


def _extract_raw_tables(html):
    chunks = []
    pattern = re.compile(r'<table[^>]*class="[^"]*wikitable[^"]*"', re.IGNORECASE)
    for m in pattern.finditer(html):
        start = m.start()
        depth = 0
        i = start
        while i < len(html):
            if html[i:i+6].lower() == "<table":
                depth += 1
                i += 6
            elif html[i:i+8].lower() == "</table>":
                depth -= 1
                if depth == 0:
                    chunks.append(html[start:i+8])
                    break
                i += 8
            else:
                i += 1
    return chunks


def _get_caption(table_html):
    m = re.search(r"<caption[^>]*>(.*?)</caption>", table_html, re.DOTALL | re.IGNORECASE)
    if m:
        return BeautifulSoup(m.group(1), "html.parser").get_text(" ", strip=True)
    return None


_SKIP_PATTERNS = re.compile(r"(?i)(voting.history|jury.vote)")


def _should_skip(table_html, caption):
    if caption and _SKIP_PATTERNS.search(caption):
        return True
    header_sample = table_html[:500].lower()
    if "voter" in header_sample or "juror" in header_sample:
        return True
    return False


def extract_tables_from_html(html):
    raw_tables = _extract_raw_tables(html)

    results = []
    for idx, table_html in enumerate(raw_tables):
        caption = _get_caption(table_html)

        if _should_skip(table_html, caption):
            log.debug("Skipping voting/jury table %d (handled by custom parser)", idx)
            continue

        size_kb = len(table_html) / 1024
        if size_kb > MAX_TABLE_HTML_KB:
            log.debug("Skipping large table %d (%s): %.0fKB", idx, caption or "no caption", size_kb)
            continue

        try:
            dfs = pd.read_html(StringIO(table_html))
            if not dfs:
                continue
            df = dfs[0]
        except Exception:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join(str(c) for c in col).strip() for col in df.columns]

        df = df.fillna("").astype(str)

        results.append({
            "caption": caption,
            "columns": list(df.columns),
            "num_rows": len(df),
            "header_hint": " ".join(str(c) for c in df.columns[:10]),
            "sample_rows": df.head(3).to_string(),
            "rows": df.to_dict(orient="records"),
        })

    return results


def extract_tables_for_season(html_path, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    html = Path(html_path).read_text(encoding="utf-8")
    tables = extract_tables_from_html(html)

    safe_name = Path(html_path).stem
    out_path = output_dir / f"{safe_name}.tables.json"
    out_path.write_text(
        json.dumps({"source": str(html_path), "tables": tables}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return tables
