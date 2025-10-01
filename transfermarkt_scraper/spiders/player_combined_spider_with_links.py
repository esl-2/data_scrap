import scrapy
import re
import json
import urllib.parse
from pathlib import Path
from datetime import datetime

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

class PlayerCombinedSpider(scrapy.Spider):
    name = "player_combined_full"
    allowed_domains = ["transfermarkt.com", "en.wikipedia.org", "ar.wikipedia.org"]

    LINKS_FILE = "players_links.json"

    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "ROBOTSTXT_OBEY": False,
    }

    def start_requests(self):
        p = Path(self.LINKS_FILE)
        if not p.exists():
            self.logger.error(f"{self.LINKS_FILE} not found. ضع الملف في نفس المجلد بصيغة JSON.")
            return

        try:
            with p.open(encoding="utf-8") as f:
                pairs = json.load(f)
        except Exception as e:
            self.logger.error(f"خطأ في قراءة {self.LINKS_FILE}: {e}")
            return

        for idx, pair in enumerate(pairs, start=262):
            tm = pair.get("transfermarkt") or pair.get("transfermarkt_url")
            wk = pair.get("wikipedia") or pair.get("wikipedia_url")
            wk_ar = pair.get("wikipedia_ar") or pair.get("wikipedia_ar_url")
            if not tm:
                self.logger.warning("تخطى إدخال دون رابط Transfermarkt: %r", pair)
                continue
            yield scrapy.Request(
                tm,
                callback=self.parse_transfermarkt,
                meta={
                    "wikipedia_url": wk,
                    "wikipedia_ar_url": wk_ar,
                    "transfermarkt_url": tm,
                    "player_id": idx
                })

    def parse_transfermarkt(self, response):
        transfermarkt_url = response.meta.get("transfermarkt_url") or response.url
        wikipedia_url = response.meta.get("wikipedia_url")
        wikipedia_ar_url = response.meta.get("wikipedia_ar_url")

        m = re.search(r'/spieler/(\d+)', transfermarkt_url)
        tm_id = m.group(1) if m else transfermarkt_url.rstrip("/").split("/")[-1]
        name_parts = response.css("h1.data-header__headline-wrapper *::text").getall()
        name = " ".join([p.strip() for p in name_parts if p.strip() and not p.strip().startswith("#")]).strip()
        main_position = response.css(".detail-position__position::text").get()
        other_positions = response.css(".detail-position__inner-box + div dl dd::text").getall()
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
                self.logger.warning(f"NO Position: {pos}")
                positions.append(pos)
        nationality_list = response.css('.data-header__items li:contains("Citizenship") .data-header__content::text').getall()
        if not nationality_list:
            nationality_list = response.css('.data-header__items li .data-header__content::text').getall()
        nationality_list = [n.strip() for n in nationality_list if n.strip()]
        nationality = nationality_list[0] if nationality_list else ""
        retired = False
        if "Retired" in response.text or "retired" in response.text:
            retired = True

        item = {
            "id": response.meta.get("player_id"),
            "transfermarkt_id": tm_id,
            "name": name,
            "name_ar": "",
            "image": "",
            "transfermarkt_url": transfermarkt_url,
            "wikipedia_url_provided": wikipedia_url,
            "positions": positions,
            "nationality": nationality,
            "retired": retired,
            "goals": "-",
            "assists": "-",
            "trophies": [],
            "career": [],
        }

        stats_url = response.css('a.content-link[href*="leistungsdaten"]::attr(href)').get()
        if stats_url:
            yield response.follow(stats_url, callback=self.parse_stats, meta={"item": item, "wikipedia_url": wikipedia_url, "wikipedia_ar_url": wikipedia_ar_url})
            return

        trophies_url = response.css('a[href*="/erfolge/spieler/"]::attr(href)').get()
        if trophies_url:
            yield response.follow(trophies_url, callback=self.parse_trophies, meta={"item": item, "wikipedia_url": wikipedia_url, "wikipedia_ar_url": wikipedia_ar_url})
            return

        if wikipedia_url:
            yield scrapy.Request(
                wikipedia_url,
                callback=self.parse_wikipedia,
                meta={"item": item, "wikipedia_ar_url": wikipedia_ar_url}
            )
        else:
            yield from self._request_wikipedia_from_name(item, wikipedia_ar_url=wikipedia_ar_url)

    def parse_stats(self, response):
        item = response.meta["item"]
        wikipedia_url = response.meta.get("wikipedia_url")
        wikipedia_ar_url = response.meta.get("wikipedia_ar_url")
        try:
            row = response.xpath('//tfoot/tr')
            if row:
                tds = row.xpath('.//td')
                if len(tds) >= 6:
                    goals = tds[-6].xpath('string(.)').get()
                    assists = tds[-5].xpath('string(.)').get()
                    if goals:
                        g = re.sub(r'[^0-9]', '', goals)
                        item["goals"] = int(g) if g.isdigit() else goals.strip()
                    if assists:
                        a = re.sub(r'[^0-9]', '', assists)
                        item["assists"] = int(a) if a.isdigit() else assists.strip()
        except Exception:
            pass

        trophies_url = response.css('a[href*="/erfolge/spieler/"]::attr(href)').get()
        if trophies_url:
            yield response.follow(trophies_url, callback=self.parse_trophies, meta={"item": item, "wikipedia_url": wikipedia_url, "wikipedia_ar_url": wikipedia_ar_url})
            return

        if wikipedia_url:
            yield scrapy.Request(
                wikipedia_url,
                callback=self.parse_wikipedia,
                meta={"item": item, "wikipedia_ar_url": wikipedia_ar_url}
            )
        else:
            yield from self._request_wikipedia_from_name(item, wikipedia_ar_url=wikipedia_ar_url)

    def parse_trophies(self, response):
        item = response.meta["item"]
        wikipedia_url = response.meta.get("wikipedia_url")
        wikipedia_ar_url = response.meta.get("wikipedia_ar_url")
        trophies = []
        try:
            for trophy_box in response.css(".box"):
                headline = trophy_box.css("h2.content-box-headline::text").get()
                if headline and "x" in headline:
                    parts = headline.split("x", 1)
                    trophy_name = parts[1].strip() if len(parts) > 1 else parts[0].strip()
                    years = []
                    for row in trophy_box.css("table.auflistung tr"):
                        year_text = row.css("td.erfolg_table_saison::text").get()
                        if year_text:
                            year = year_text.strip()
                            if "/" in year:
                                try:
                                    partsy = year.split("/")
                                    year_conv = "20" + partsy[1][-2:]
                                    years.append(int(year_conv))
                                except Exception:
                                    pass
                            else:
                                try:
                                    years.append(int(year))
                                except Exception:
                                    pass
                    trophies.append({"tournament": trophy_name, "years": years})
        except Exception:
            pass

        item["trophies"] = trophies
        if wikipedia_url:
            yield scrapy.Request(
                wikipedia_url,
                callback=self.parse_wikipedia,
                meta={"item": item, "wikipedia_ar_url": wikipedia_ar_url}
            )
        else:
            yield from self._request_wikipedia_from_name(item, wikipedia_ar_url=wikipedia_ar_url)

    def _request_wikipedia_from_name(self, item, wikipedia_ar_url=None):
        name = item.get("name", "")
        if not name:
            yield item
            return
        slug = urllib.parse.quote(name.replace(" ", "_"))
        wiki_url = f"https://en.wikipedia.org/wiki/{slug}"
        item["wikipedia_url_provided"] = wiki_url
        yield scrapy.Request(
            wiki_url,
            callback=self.parse_wikipedia,
            meta={"item": item, "wikipedia_ar_url": wikipedia_ar_url}
        )

    def parse_wikipedia(self, response):
        item = response.meta["item"]
        wikipedia_ar_url = response.meta.get("wikipedia_ar_url")
        try:
            infobox = response.css("table.infobox")
            image_url = infobox.css("td.infobox-image img::attr(src)").get()
            if image_url:
                if image_url.startswith("//"):
                    image_url = "https:" + image_url
                elif image_url.startswith("/"):
                    image_url = "https://en.wikipedia.org" + image_url
                item["image"] = image_url
        except Exception:
            pass

        career = []
        try:
            infobox = response.css("table.infobox")
            rows = []
            if infobox:
                rows = infobox.css("tr")
            else:
                rows = response.xpath('//table[contains(.,"Years") and contains(.,"Team")]//tr')
            start_idx = None
            for i, tr in enumerate(rows):
                header_text = tr.xpath('string(.)').get() or ""
                if re.search(r'senior career|senior team|club career', header_text, flags=re.I):
                    start_idx = i + 1
                    break
                if re.search(r'\bYears\b', header_text, flags=re.I) and re.search(r'\bTeam\b', header_text, flags=re.I):
                    start_idx = i + 1
                    break
            if start_idx is None:
                start_idx = 0
            for tr in rows[start_idx:]:
                th_text = tr.css("th::text, th span::text").get() or ""
                if th_text and re.search(r'international career|youth career|national team|personal information', th_text, flags=re.I):
                    break
                cells = [c.strip() for c in tr.xpath('.//th//text() | .//td//text()').getall() if c.strip()]
                if not cells:
                    continue
                header_join = " ".join(cells).lower()
                if "years" in header_join and "team" in header_join:
                    continue
                years_raw = cells[0] if len(cells) >= 1 else ""
                team_raw = cells[1] if len(cells) >= 2 else ""
                apps_raw = cells[2] if len(cells) >= 3 else ""
                goals_raw = cells[3] if len(cells) >= 4 else ""
                def looks_like_years(txt):
                    if not txt:
                        return False
                    if re.search(r'\d{4}', txt):
                        return True
                    if re.search(r'\b(' + '|'.join(MONTHS_FULL.keys()) + r'|' + '|'.join(MONTHS_ABBR.keys()) + r')\b', txt.lower()):
                        return True
                    return False
                if not looks_like_years(years_raw) and looks_like_years(team_raw):
                    if re.search(r'[A-Za-z]', years_raw):
                        team_raw = years_raw
                        years_raw = ""
                team_clean = clean_team_name(team_raw)
                if not team_clean:
                    raw_team_td = tr.xpath('string(.//td)').get() or ""
                    if raw_team_td:
                        team_clean = clean_team_name(raw_team_td)
                years_norm = normalize_years_field(years_raw)
                apps = apps_raw.strip()
                goals = goals_raw.strip()
                if (apps and re.search(r'[A-Za-z]', apps) and not re.search(r'\d', apps)) and (not team_clean):
                    team_clean = clean_team_name(apps)
                    apps = ""
                team_clean = team_clean.strip()
                if years_norm.lower() == "years" and (not team_clean):
                    continue
                if not (years_norm or team_clean):
                    continue
                entry = {"years": years_norm, "team": team_clean}
                if apps:
                    entry["apps"] = apps
                if goals:
                    entry["goals"] = goals
                career.append(entry)
            if not career:
                tables = response.xpath('//table[contains(.,"Years") and contains(.,"Team")]')
                if tables:
                    table = tables[0]
                    for tr in table.xpath('.//tr'):
                        cells = [x.strip() for x in tr.xpath('.//th//text() | .//td//text()').getall() if x.strip()]
                        if len(cells) >= 2:
                            years_raw = cells[0]
                            team_raw = cells[1]
                            years_norm = normalize_years_field(years_raw)
                            team_clean = clean_team_name(team_raw)
                            if not (years_norm or team_clean):
                                continue
                            entry = {"years": years_norm, "team": team_clean}
                            if len(cells) >= 3:
                                entry["apps"] = cells[2]
                            if len(cells) >= 4:
                                entry["goals"] = cells[3]
                            career.append(entry)
        except Exception:
            pass
        item["career"] = career

        # طلب ويكيبيديا العربية إذا وُجد الرابط
        if wikipedia_ar_url:
            yield scrapy.Request(
                wikipedia_ar_url,
                callback=self.parse_wikipedia_ar,
                meta={"item": item}
            )
        else:
            yield item

    def parse_wikipedia_ar(self, response):
        item = response.meta["item"]
        try:
            name_ar = response.css("h1#firstHeading::text").get()
            if name_ar:
                item["name_ar"] = name_ar.strip()
            else:
                title = response.css("title::text").get()
                if title:
                    name_ar = title.split("-")[0].strip()
                    item["name_ar"] = name_ar
        except Exception:
            pass
        yield item

# تشغيل العنكبوت:
# scrapy runspider player_combined_spider_with_links.py -o players.json