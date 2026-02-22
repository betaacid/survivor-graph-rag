import logging
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

API = "https://en.wikipedia.org/w/api.php"
RATE_LIMIT_SECONDS = 0.5
HEADERS = {"User-Agent": "SurvivorGraphRAG/1.0 (https://github.com/survivorgraph; contact@example.com)"}

log = logging.getLogger(__name__)


def get_season_titles():
    category = "Category:Survivor_(American_TV_series)_seasons"
    titles = []
    cmcontinue = None

    while True:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": category,
            "cmtype": "page",
            "cmlimit": "500",
            "format": "json",
        }
        if cmcontinue:
            params["cmcontinue"] = cmcontinue

        resp = requests.get(API, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as e:
            log.error("Wikipedia API returned non-JSON. Status=%s, body[:500]=%r", resp.status_code, resp.text[:500])
            raise
        members = data["query"]["categorymembers"]
        titles.extend(m["title"] for m in members)

        cmcontinue = data.get("continue", {}).get("cmcontinue")
        if not cmcontinue:
            break

    season_titles = [
        t for t in titles
        if t.startswith("Survivor") and "season" not in t.lower()
        and "list" not in t.lower() and "category" not in t.lower()
    ]
    return sorted(season_titles)


def fetch_parsed_html(title):
    params = {
        "action": "parse",
        "page": title,
        "prop": "text",
        "format": "json",
        "redirects": "1",
    }
    resp = requests.get(API, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    try:
        data = resp.json()
    except ValueError as e:
        log.error("Wikipedia API returned non-JSON. Status=%s, body[:500]=%r", resp.status_code, resp.text[:500])
        raise
    page = data["parse"]
    return page["pageid"], page["text"]["*"]


def html_to_plain_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.select("table, .navbox, .mw-editsection, style, script"):
        tag.decompose()
    text = soup.get_text("\n")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def download_all_seasons(output_dir, limit=None, fresh=False):
    output_dir = Path(output_dir)
    html_dir = output_dir / "raw_html"
    text_dir = output_dir / "raw_text"
    html_dir.mkdir(parents=True, exist_ok=True)
    text_dir.mkdir(parents=True, exist_ok=True)

    titles = get_season_titles()
    if limit is not None:
        titles = titles[:limit]
        log.info("Smoke test mode: limiting to %d season(s)", limit)
    log.info("Found %d season page(s) to process", len(titles))

    skip_cache = fresh or limit is not None
    results = []
    for title in titles:
        safe_name = title.replace(" ", "_").replace(":", "_").replace("/", "_")
        html_path = html_dir / f"{safe_name}.html"
        text_path = text_dir / f"{safe_name}.txt"

        if not skip_cache and html_path.exists() and text_path.exists():
            log.info("  [cached] %s", title)
            results.append({
                "title": title,
                "html_path": str(html_path),
                "text_path": str(text_path),
            })
            continue

        pageid, html = fetch_parsed_html(title)
        plain_text = html_to_plain_text(html)

        html_path.write_text(html, encoding="utf-8")
        text_path.write_text(plain_text, encoding="utf-8")

        url = "https://en.wikipedia.org/wiki/" + title.replace(" ", "_")
        results.append({
            "title": title,
            "pageid": pageid,
            "url": url,
            "html_path": str(html_path),
            "text_path": str(text_path),
        })
        log.info("  [downloaded] %s", title)
        time.sleep(RATE_LIMIT_SECONDS)

    return results
