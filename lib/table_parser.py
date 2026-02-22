import json
import re
from io import StringIO
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup


def extract_tables_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    wiki_tables = soup.find_all("table", class_="wikitable")

    results = []
    for table in wiki_tables:
        caption_tag = table.find("caption")
        caption = caption_tag.get_text(" ", strip=True) if caption_tag else None

        try:
            dfs = pd.read_html(StringIO(str(table)))
            if not dfs:
                continue
            df = dfs[0]
        except Exception:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join(str(c) for c in col).strip() for col in df.columns]

        df = df.fillna("").astype(str)

        header_text = " ".join(str(c) for c in df.columns[:10])
        first_rows = df.head(3).to_string()

        results.append({
            "caption": caption,
            "columns": list(df.columns),
            "num_rows": len(df),
            "header_hint": header_text,
            "sample_rows": first_rows,
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
