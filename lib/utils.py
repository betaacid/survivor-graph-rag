import re


def extract_season_number(title):
    match = re.search(r"[Ss]eason\s*(\d+)", title)
    if match:
        return int(match.group(1))

    known = {
        "Borneo": 1,
        "The Australian Outback": 2,
        "Africa": 3,
        "Marquesas": 4,
        "Thailand": 5,
        "The Amazon": 6,
        "Pearl Islands": 7,
        "All-Stars": 8,
        "Vanuatu": 9,
        "Palau": 10,
        "Guatemala": 11,
        "Panama": 12,
        "Cook Islands": 13,
        "Fiji": 14,
        "China": 15,
        "Micronesia": 16,
        "Gabon": 17,
        "Tocantins": 18,
        "Samoa": 19,
        "Heroes vs. Villains": 20,
        "Nicaragua": 21,
        "Redemption Island": 22,
        "South Pacific": 23,
        "One World": 24,
        "Philippines": 25,
        "Caramoan": 26,
        "Blood vs. Water": 27,
        "Cagayan": 28,
        "San Juan del Sur": 29,
        "Worlds Apart": 30,
        "Cambodia": 31,
        "Kaôh Rōng": 32,
        "Millennials vs. Gen X": 33,
        "Game Changers": 34,
        "Heroes vs. Healers vs. Hustlers": 35,
        "Ghost Island": 36,
        "David vs. Goliath": 37,
        "Edge of Extinction": 38,
        "Island of the Idols": 39,
        "Winners at War": 40,
        "41": 41,
        "42": 42,
        "43": 43,
        "44": 44,
        "45": 45,
        "46": 46,
        "47": 47,
        "48": 48,
        "49": 49,
    }
    lower = title.lower()
    for subtitle, num in known.items():
        if subtitle.lower() in lower:
            return num

    nums = re.findall(r"(\d+)", title)
    for n in nums:
        val = int(n)
        if 1 <= val <= 49:
            return val

    return None
