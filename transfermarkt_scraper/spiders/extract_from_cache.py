# يمكنك حفظه باسم extract_from_cache.py
import os
from parsel import Selector
import re
import json
import gzip

# نسخ الدوال المساعدة من سبيدر
MONTHS_FULL = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12"
}
MONTHS_ABBR = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12"
}
POSITION_ABBREVIATIONS = {
    "Goalkeeper": "GK",
    "Centre-Back": "CB",
    "Left-Back": "LB",
    "Right-Back": "RB",
    "Defensive Midfield": "DM",
    "Central Midfield": "CM",
    "Attacking Midfield": "AM",
    "Left Winger": "LW",
    "Right Winger": "RW",
    "Second Striker": "SS",
    "Centre-Forward": "CF",
    "Striker": "ST",
    "Left Midfield": "LM",
    "Right Midfield": "RM",
    "Left Wing-Back": "LWB",
    "Right Wing-Back": "RWB",
    "Sweeper": "SW",
    "Forward": "FW",
}

def try_parse_day_month_year(part: str):
    if not part or not part.strip():
        return None
    s = part.strip()
    s = re.sub(r'\s+', ' ', s.replace(',', ' ')).strip()
    m_iso = re.match(r'^\s*(\d{4})[-/](\d{1,2})[-/](\d{1,2})\s*$', s)
    if m_iso:
        y, mo, d = m_iso.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    m1 = re.match(r'^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$', s)
    if m1:
        d, mon, y = m1.groups()
        mon_l = mon.lower()
        mo = MONTHS_FULL.get(mon_l[:]) or MONTHS_ABBR.get(mon_l[:3])
        if mo:
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    m2 = re.match(r'^([A-Za-z]+)\s+(\d{1,2})\s+(\d{4})$', s)
    if m2:
        mon, d, y = m2.groups()
        mon_l = mon.lower()
        mo = MONTHS_FULL.get(mon_l) or MONTHS_ABBR.get(mon_l[:3])
        if mo:
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    m3 = re.match(r'^(\d{1,2})[./\-](\d{1,2})[./\-](\d{4})$', s)
    if m3:
        d, mo, y = m3.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    m4 = re.match(r'^\d{4}$', s)
    if m4:
        return s
    return None

def normalize_years_field(years_raw: str):
    # ... نفس الدالة من السبيدر ...
    if not years_raw:
        return ""
    s = years_raw.strip()
    s = s.replace('\u00A0', ' ')
    parts = re.split(r'\s*[–—\-]\s*|\s+to\s+', s)
    parts = [p.strip() for p in parts if p.strip()]
    if not parts:
        return s
    if len(parts) == 1:
        p = parts[0]
        iso = try_parse_day_month_year(p)
        return iso if iso else p
    left = parts[0]
    right = parts[1] if len(parts) > 1 else ""
    left_iso = try_parse_day_month_year(left)
    right_iso = try_parse_day_month_year(right)
    if left_iso and right_iso:
        return f"{left_iso} – {right_iso}"
    if left_iso and not right_iso and re.match(r'^\d{4}$', right):
        return f"{left_iso} – {right}"
    if not left_iso and right_iso:
        return f"{left} – {right_iso}"
    return s

def clean_team_name(raw: str):
    # ... نفس الدالة من السبيدر ...
    if not raw:
        return ""
    t = raw.strip()
    t = re.sub(r'^\s*→\s*', '', t)
    t = re.sub(r'\(loan\)', '', t, flags=re.I)
    t = re.sub(r'\(on loan\)', '', t, flags=re.I)
    t = re.sub(r'\bon loan\b', '', t, flags=re.I)
    t = re.sub(r'\(.*?loan.*?\)', '', t, flags=re.I)
    t = re.sub(r'\s*\(\s*\)\s*', '', t)
    return t.strip()

CACHE_DIR = r'd:\projects\Python\scrap\transfermarkt_scraper\.scrapy\httpcache'

def extract_transfermarkt_data(response_body):
    selector = Selector(response_body)
    name_parts = selector.css("h1.data-header__headline-wrapper *::text").getall()
    name = " ".join([p.strip() for p in name_parts if p.strip() and not p.strip().startswith("#")]).strip()
    main_position = selector.css(".detail-position__position::text").get()
    other_positions = selector.css(".detail-position__inner-box + div dl dd::text").getall()
    raw_positions = []
    if main_position:
        raw_positions.append(main_position.strip())
    raw_positions += [p.strip() for p in other_positions if p and p.strip()]
    positions = []
    for pos in raw_positions:
        abbreviation = POSITION_ABBREVIATIONS.get(pos)
        if abbreviation:
            positions.append(abbreviation)
        else:
            positions.append(pos)
    nationality_list = selector.css('.data-header__items li:contains("Citizenship") .data-header__content::text').getall()
    if not nationality_list:
        nationality_list = selector.css('.data-header__items li .data-header__content::text').getall()
    nationality_list = [n.strip() for n in nationality_list if n.strip()]
    nationality = nationality_list[0] if nationality_list else ""
    retired = False
    if "Retired" in response_body or "retired" in response_body:
        retired = True
    return {
        "name": name,
        "positions": positions,
        "nationality": nationality,
        "retired": retired,
    }

def main():
    results = []
    for root, dirs, files in os.walk(CACHE_DIR):
        for file in files:
            if file == 'response_body':
                file_path = os.path.join(root, file)
                headers_path = os.path.join(root, 'response_headers')
                # اقرأ الهيدر لمعرفة إذا كان الملف مضغوط
                is_gzipped = False
                if os.path.exists(headers_path):
                    with open(headers_path, 'r', encoding='utf-8', errors='ignore') as h:
                        headers = h.read()
                        if 'Content-Encoding: gzip' in headers:
                            is_gzipped = True
                with open(file_path, 'rb') as f:
                    raw = f.read()
                    try:
                        if is_gzipped:
                            response_body = gzip.decompress(raw)
                        else:
                            response_body = raw
                        # جرب طباعة أول جزء للتأكد من أنه HTML
                        print(response_body[:500])
                        data = extract_transfermarkt_data(response_body.decode('utf-8', errors='ignore'))
                        results.append(data)
                    except Exception as e:
                        print(f'Error parsing {file_path}: {e}')
    # حفظ النتائج في ملف
    with open('extracted_players.json', 'w', encoding='utf-8') as out:
        json.dump(results, out, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    main()