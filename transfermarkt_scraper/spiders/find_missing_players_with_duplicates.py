#!/usr/bin/env python3
"""
find_missing_players_with_duplicates.py

Usage:
    python find_missing_players_with_duplicates.py source.json target.json [--fuzzy 0.90]

Outputs:
  - missing_players.json        (اللاعبون المفقودون + اللاعبين المشار إليهم كمكرر)
  - missing_players.csv         (نفس المحتوى CSV)
  - duplicate_players.json      (مجمّعات التكرارات في source و target)
"""
import sys, json, unicodedata, re, csv, argparse
from pathlib import Path
from difflib import SequenceMatcher

def load_json_path(p: Path):
    text = p.read_text(encoding='utf-8')
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return [data]
        return data
    except json.JSONDecodeError:
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

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def key_for_player(p):
    # return a list of possible keys (primary tid key if exists, else name key)
    tid = p.get("transfermarkt_id") or p.get("transfermarktId") or p.get("transfermarkt id") or p.get("transfermarkt")
    if tid is None:
        raw_id = p.get("id")
        if isinstance(raw_id, int) or (isinstance(raw_id, str) and raw_id.isdigit()):
            tid = raw_id
    keys = []
    if tid is not None:
        keys.append(f"tid::{str(tid)}")
    name = p.get("name") or ""
    nn = normalize_name(name)
    if nn:
        keys.append(f"name::{nn}")
    return keys

def find_duplicates(players):
    by_key = {}
    for idx, p in enumerate(players):
        keys = key_for_player(p)
        # if there are keys, register under each
        if not keys:
            # fallback: empty name index by position
            k = f"index::{idx}"
            by_key.setdefault(k, []).append({"index": idx, "player": p})
        else:
            for k in keys:
                by_key.setdefault(k, []).append({"index": idx, "player": p})
    # collect only groups with more than 1 occurrence, but ensure we don't duplicate groups
    groups = []
    seen_member_ids = set()
    for k, lst in by_key.items():
        if len(lst) > 1:
            # create a stable id for group by concatenating indices
            member_ids = tuple(sorted(item["index"] for item in lst))
            if member_ids in seen_member_ids:
                continue
            seen_member_ids.add(member_ids)
            groups.append({
                "key": k,
                "indices": [item["index"] for item in lst],
                "members": [item["player"] for item in lst]
            })
    return groups

def build_target_lookup(target_players):
    ids = set()
    normnames = set()
    for p in target_players:
        tid = p.get("transfermarkt_id") or p.get("transfermarktId") or p.get("transfermarkt id") or p.get("transfermarkt")
        if tid is None:
            raw_id = p.get("id")
            if isinstance(raw_id, int) or (isinstance(raw_id, str) and raw_id.isdigit()):
                tid = raw_id
        if tid is not None:
            ids.add(str(tid))
        name = p.get("name") or ""
        nn = normalize_name(name)
        if nn:
            normnames.add(nn)
    return ids, normnames

def find_missing_and_flag_duplicates(source, target_ids, target_normnames,
                                     dup_src_groups, dup_tgt_groups, fuzzy_threshold=0.90):
    # build quick lookup sets for duplicate keys (for membership test)
    dup_src_keys = set(g["key"] for g in dup_src_groups)
    dup_tgt_keys = set(g["key"] for g in dup_tgt_groups)

    # also build mapping from key to group object for including group members
    dup_src_map = {g["key"]: g for g in dup_src_groups}
    dup_tgt_map = {g["key"]: g for g in dup_tgt_groups}

    missing = []
    for p in source:
        tid = p.get("transfermarkt_id") or p.get("transfermarktId") or p.get("transfermarkt id") or p.get("transfermarkt")
        if tid is None:
            raw_id = p.get("id")
            if isinstance(raw_id, int) or (isinstance(raw_id, str) and raw_id.isdigit()):
                tid = raw_id
        tid_s = str(tid) if tid is not None else None

        found = False
        # 1) id exact
        if tid_s and tid_s in target_ids:
            found = True

        # 2) normalized name exact
        name = p.get("name") or ""
        nn = normalize_name(name)
        if not found and nn and nn in target_normnames:
            found = True

        # 3) fuzzy name
        if not found and nn:
            best = 0.0
            for cand in target_normnames:
                r = similar(nn, cand)
                if r > best:
                    best = r
            if best >= fuzzy_threshold:
                found = True

        # build base entry
        entry = {
            "name": p.get("name"),
            "name_ar": p.get("name_ar"),
            "transfermarkt_url": p.get("transfermarkt_url") or p.get("transfermarktUrl"),
            "wikipedia_url_provided": p.get("wikipedia_url_provided") or p.get("wikipedia_url") or p.get("wikipedia"),
            "transfermarkt_id": tid_s
        }

        # determine duplicate membership (check possible keys)
        keys = key_for_player(p)
        dup_locations = []
        dup_info = []
        for k in keys:
            if k in dup_src_keys:
                dup_locations.append("source")
                dup_info.append({"key": k, "group": dup_src_map.get(k)})
            if k in dup_tgt_keys:
                dup_locations.append("target")
                dup_info.append({"key": k, "group": dup_tgt_map.get(k)})
        # remove duplicates in dup_locations
        dup_locations = sorted(set(dup_locations))

        if dup_locations:
            entry["duplicate"] = True
            entry["duplicate_in"] = dup_locations  # could be ["source"], ["target"], or both
            # include summary of duplicate group members (useful for manual review)
            entry["duplicate_groups"] = []
            for info in dup_info:
                g = info.get("group")
                if g:
                    # include only compact member info to avoid huge output
                    entry["duplicate_groups"].append({
                        "key": info.get("key"),
                        "member_count": len(g.get("members", [])),
                        "members_preview": [
                            {"name": m.get("name"), "transfermarkt_id": (m.get("transfermarkt_id") or m.get("id"))}
                            for m in g.get("members", [])[:5]
                        ]
                    })

        # if not found -> missing
        if not found:
            entry["missing"] = True
            missing.append(entry)
        else:
            # if found but duplicate flag exists and user wanted duplicates listed in missing too,
            # user asked that duplicates be "written in missing_players.json" — we include them only if duplicate
            if dup_locations:
                entry["missing"] = False
                missing.append(entry)
            # else do nothing (found and not duplicate)
    return missing

def write_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')

def write_csv(path: Path, data):
    if not data:
        return
    # flatten keys for CSV: take union of keys from all rows
    keys = set()
    for d in data:
        keys.update(d.keys())
    keys = list(keys)
    with path.open("w", encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in data:
            writer.writerow({k: ("" if row.get(k) is None else (json.dumps(row.get(k), ensure_ascii=False) if isinstance(row.get(k), (list, dict)) else row.get(k))) for k in keys})

def main(argv):
    parser = argparse.ArgumentParser(description="Find missing players and flag duplicates (without deleting).")
    parser.add_argument("source")
    parser.add_argument("target")
    parser.add_argument("--fuzzy", type=float, default=0.90, help="Fuzzy match threshold (0..1). Default 0.90")
    args = parser.parse_args(argv[1:])

    src_path = Path(args.source)
    tgt_path = Path(args.target)
    if not src_path.exists() or not tgt_path.exists():
        print("أحد الملفين غير موجود. تأكد من المسارات.")
        return

    src = load_json_path(src_path)
    tgt = load_json_path(tgt_path)
    print(f"Loaded {len(src)} players from {src_path.name}")
    print(f"Loaded {len(tgt)} players from {tgt_path.name}")

    dup_src_groups = find_duplicates(src)
    dup_tgt_groups = find_duplicates(tgt)

    duplicate_players = {
        "source_duplicates": dup_src_groups,
        "target_duplicates": dup_tgt_groups
    }
    write_json(Path("duplicate_players.json"), duplicate_players)
    print(f"Wrote duplicate_players.json  (source groups: {len(dup_src_groups)}, target groups: {len(dup_tgt_groups)})")

    target_ids, target_normnames = build_target_lookup(tgt)
    missing_and_dup_flagged = find_missing_and_flag_duplicates(src, target_ids, target_normnames,
                                                               dup_src_groups, dup_tgt_groups,
                                                               fuzzy_threshold=args.fuzzy)

    write_json(Path("missing_players.json"), missing_and_dup_flagged)
    write_csv(Path("missing_players.csv"), missing_and_dup_flagged)

    print()
    print(f"Done. Wrote missing_players.json (entries: {len(missing_and_dup_flagged)}) and missing_players.json")

if __name__ == "__main__":
    main(sys.argv)
