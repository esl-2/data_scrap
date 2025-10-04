#!/usr/bin/env python3
"""
find_common_players_single_file.py

Usage:
    python find_common_players_single_file.py source.json target.json

Output:
    - common_players.json  (contains groups of duplicated/same players across and inside files)
"""
import sys, json, unicodedata, re
from pathlib import Path

def load_json_path(p: Path):
    text = p.read_text(encoding='utf-8')
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return [data]
        return data
    except json.JSONDecodeError:
        # try NDJSON
        items = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return items

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    no_diac = "".join(c for c in nfkd if not unicodedata.combining(c))
    s = no_diac.lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def canonical_keys(player):
    """Return list of canonical keys for grouping (tid preferred, then normalized name)."""
    keys = []
    tid = player.get("transfermarkt_id") or player.get("transfermarktId") or player.get("transfermarkt id") or player.get("transfermarkt")
    if tid is None:
        raw_id = player.get("id")
        if isinstance(raw_id, int) or (isinstance(raw_id, str) and raw_id.isdigit()):
            tid = raw_id
    if tid is not None:
        keys.append(f"tid::{str(tid)}")
    name = player.get("name") or ""
    nn = normalize_name(name)
    if nn:
        keys.append(f"name::{nn}")
    return keys

def compact_player_view(p):
    """Return small dict of useful fields for output occurrences."""
    return {
        "name": p.get("name"),
        "name_ar": p.get("name_ar"),
        "transfermarkt_id": (p.get("transfermarkt_id") or p.get("id") or None),
        "transfermarkt_url": p.get("transfermarkt_url") or p.get("transfermarktUrl"),
        "wikipedia_url_provided": p.get("wikipedia_url_provided") or p.get("wikipedia_url") or p.get("wikipedia")
    }

def find_common_groups(source_list, target_list):
    combined = []
    for i, p in enumerate(source_list):
        combined.append(("source", i, p))
    for i, p in enumerate(target_list):
        combined.append(("target", i, p))

    groups_map = {}  # key -> list of occurrences
    for filetag, idx, p in combined:
        keys = canonical_keys(p)
        if not keys:
            # fallback: use raw json signature if no key
            k = f"raw::{filetag}::{idx}"
            groups_map.setdefault(k, []).append({"file": filetag, "index": idx, "player": p})
        else:
            for k in keys:
                groups_map.setdefault(k, []).append({"file": filetag, "index": idx, "player": p})

    # combine groups that represent the same set of occurrences (avoid duplicates across keys)
    seen_signatures = {}
    final_groups = []
    for k, occ in groups_map.items():
        sig = tuple(sorted((o["file"], o["index"]) for o in occ))
        if len(sig) <= 1:
            continue
        if sig in seen_signatures:
            # append key to existing
            seen_signatures[sig]["keys"].append(k)
        else:
            g = {
                "keys": [k],
                "total_count": len(occ),
                "occurrences": [
                    {
                        "file": o["file"],
                        "index": o["index"],
                        "player": compact_player_view(o["player"])
                    } for o in occ
                ],
            }
            present_in = sorted(list({o["file"] for o in occ}))
            g["present_in"] = present_in
            final_groups.append(g)
            seen_signatures[sig] = g
    return final_groups

def main(argv):
    if len(argv) < 3:
        print("Usage: python find_common_players_single_file.py source.json target.json")
        return
    src_path = Path(argv[1])
    tgt_path = Path(argv[2])
    if not src_path.exists() or not tgt_path.exists():
        print("أحد الملفين غير موجود. تأكد من المسارات.")
        return

    src = load_json_path(src_path)
    tgt = load_json_path(tgt_path)
    print(f"Loaded {len(src)} players from {src_path.name}")
    print(f"Loaded {len(tgt)} players from {tgt_path.name}")

    groups = find_common_groups(src, tgt)

    out = Path("common_players.json")
    out.write_text(json.dumps(groups, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"Wrote {out}  (groups: {len(groups)})")

if __name__ == "__main__":
    main(sys.argv)
