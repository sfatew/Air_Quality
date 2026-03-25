import json
import pandas as pd

with open("output.json", "r") as f:
    data = json.load(f)

entries = data['feed']['entry']
for entry in entries:
    links = entry['links']
    for link in links:
        href = link['href']
        rel = link['rel']
        inheritied = link.get("inherited", None)
        if rel.endswith("data#") and href.endswith(".nc") and href.startswith("https"):
            print(link)