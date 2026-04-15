#!/usr/bin/env python3
"""Extract restaurant data from OSM Overpass API web_fetch result."""
import json, sys

input_path = sys.argv[1]
output_path = sys.argv[2]

with open(input_path) as f:
    wrapper = json.load(f)

raw_text = wrapper[0]["text"] if isinstance(wrapper, list) else wrapper.get("text", "")
# Strip HTTP headers if present
if raw_text.startswith("HTTP"):
    # Find the start of the JSON body after blank line
    idx = raw_text.find("\n\n")
    if idx != -1:
        raw_text = raw_text[idx+2:]
data = json.loads(raw_text)
elements = data.get("elements", [])
print(f"Total OSM elements: {len(elements)}")

restaurants = []
for e in elements:
    tags = e.get("tags", {})
    name = tags.get("name")
    website = tags.get("website", "")
    if not name or not website:
        continue
    if not website.startswith("http"):
        website = "https://" + website
    restaurants.append({
        "name": name,
        "website": website,
        "cuisine": tags.get("cuisine", ""),
        "city": tags.get("addr:city", "San Francisco"),
    })

print(f"Restaurants with websites: {len(restaurants)}")
with open(output_path, "w") as f:
    json.dump(restaurants, f)
print(f"Saved to {output_path}")
