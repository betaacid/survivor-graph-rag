import logging
import re

from bs4 import BeautifulSoup, Tag

log = logging.getLogger(__name__)

SKIP_VALUES = frozenset({
    "", "none", "saved", "immune", "tied", "won", "lost", "n/a",
})


def _clean_cell_text(cell):
    for sup in cell.find_all("sup"):
        sup.decompose()
    text = cell.get_text(strip=True)
    text = re.sub(r"\[.*?\]", "", text)
    return text.strip()


def _find_voting_history_table(soup):
    heading = soup.find(id="Voting_history")
    if not heading:
        heading = soup.find(id="Voting_History")
    if not heading:
        for h2 in soup.find_all(["h2", "h3"]):
            if "voting history" in h2.get_text().lower():
                heading = h2
                break
    if not heading:
        return None

    node = heading
    while node:
        node = node.find_next_sibling() if hasattr(node, "find_next_sibling") else None
        if node is None:
            node = heading.parent
            if node:
                node = node.find_next_sibling()
        if node is None:
            break
        if isinstance(node, Tag) and node.name == "table" and "wikitable" in (node.get("class") or []):
            return node
        if isinstance(node, Tag) and node.name in ("h2", "h3"):
            break
    return None


def _build_episode_map(episode_row):
    col_idx = 0
    episode_map = {}
    cells = episode_row.find_all(["th", "td"])

    for cell in cells:
        text = _clean_cell_text(cell)
        if text.lower() == "episode":
            col_idx = 0
            continue

        colspan = int(cell.get("colspan", 1))

        numbers = re.findall(r"\d+", text)
        if numbers:
            ep_num = int(numbers[0])
            for i in range(colspan):
                episode_map[col_idx + i] = ep_num
        col_idx += colspan

    return episode_map


def _find_separator_row(table):
    for tr in table.find_all("tr"):
        td = tr.find("td")
        if td and td.get("bgcolor") == "black":
            return tr
        if td and "background:black" in (td.get("style") or ""):
            return tr
        th = tr.find("th")
        if th and "background:black" in (th.get("style") or ""):
            return tr
    return None


def parse_voting_history(html):
    soup = BeautifulSoup(html, "html.parser") if isinstance(html, str) else html
    table = _find_voting_history_table(soup)
    if table is None:
        log.debug("Voting history table not found")
        return None

    episode_row = None
    for tr in table.find_all("tr"):
        first = tr.find(["th", "td"])
        if first and _clean_cell_text(first).lower() == "episode":
            episode_row = tr
            break

    if episode_row is None:
        log.warning("Episode header row not found in voting history table")
        return None

    episode_map = _build_episode_map(episode_row)
    if not episode_map:
        log.warning("No episode numbers found in voting history table")
        return None

    separator = _find_separator_row(table)
    if separator is None:
        log.warning("Black separator row not found in voting history table")
        return None

    votes = []
    seen = set()
    in_voter_section = False
    for tr in separator.find_next_siblings("tr"):
        th = tr.find("th")
        if not th:
            continue

        label = _clean_cell_text(th)

        if label.lower() in ("voter", "juror", ""):
            in_voter_section = True
            continue

        if not in_voter_section:
            continue

        voter = label
        tds = tr.find_all("td")
        col_idx = 0
        for td in tds:
            colspan = int(td.get("colspan", 1))
            target = _clean_cell_text(td)

            if target.lower() not in SKIP_VALUES and target:
                italic = td.find("i")
                if italic and _clean_cell_text(italic).lower() in SKIP_VALUES:
                    col_idx += colspan
                    continue

                ep_num = episode_map.get(col_idx)
                if ep_num is not None:
                    key = (voter, ep_num, target)
                    if key not in seen:
                        seen.add(key)
                        votes.append({
                            "voter": voter,
                            "episode_number": ep_num,
                            "target": target,
                        })

            col_idx += colspan

    if not votes:
        log.warning("No votes parsed from voting history table")
        return None

    log.info("Parsed %d votes from voting history", len(votes))
    return {"votes": votes}


def _find_jury_vote_table(soup):
    for table in soup.find_all("table", class_="wikitable"):
        for th in table.find_all("th"):
            if "jury vote" in th.get_text().lower():
                return table
    return None


def parse_jury_vote(html):
    soup = BeautifulSoup(html, "html.parser") if isinstance(html, str) else html
    table = _find_jury_vote_table(soup)
    if table is None:
        log.debug("Jury vote table not found")
        return None

    finalists = []
    separator_found = False
    in_juror_section = False
    jury_votes = []

    for tr in table.find_all("tr"):
        th = tr.find("th")
        if not th:
            continue

        label = _clean_cell_text(th)

        if label.lower() == "finalist":
            tds = tr.find_all("td")
            finalists = [_clean_cell_text(td) for td in tds]
            continue

        if th.get("style") and "background:black" in th.get("style", ""):
            separator_found = True
            continue

        td_with_black = tr.find("td", attrs={"bgcolor": "black"})
        if td_with_black:
            separator_found = True
            continue

        if label.lower() in ("juror", "jury member"):
            in_juror_section = True
            continue

        if not separator_found and not in_juror_section:
            continue

        if not finalists:
            continue

        juror = label
        tds = tr.find_all("td")

        voted_for = None
        for i, td in enumerate(tds):
            has_check = (
                td.get("data-sort-value") == "Yes"
                or td.find("img", alt="Yes")
                or "#DFD" in (td.get("style") or "").upper()
                or "table-yes" in " ".join(td.get("class") or [])
            )
            if has_check and i < len(finalists):
                voted_for = finalists[i]
                break

        if voted_for and juror:
            jury_votes.append({"juror": juror, "voted_for": voted_for})

    if not jury_votes:
        log.warning("No jury votes parsed from jury vote table")
        return None

    log.info("Parsed %d jury votes", len(jury_votes))
    return {"jury_votes": jury_votes}
