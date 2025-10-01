import json
import sys

def extract_links(input_filename, output_filename="linkes"):
    with open(input_filename, "r", encoding="utf-8") as infile:
        data = json.load(infile)
    
    links_list = []
    for player in data:
        links_list.append({
            "transfermarkt": player.get("transfermarkt_url", ""),
            "wikipedia": player.get("wikipedia_url_provided", ""),
            "wikipedia_ar": ""
        })

    with open(output_filename, "w", encoding="utf-8") as outfile:
        json.dump(links_list, outfile, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_links.py <input_json_file> [output_file]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "linkes"
    extract_links(input_file, output_file)