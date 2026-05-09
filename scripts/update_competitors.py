import json

competitors_update = {
    "coleman-furniture": {"status": "Ready to process"},
    "home-gallery-stores": {"status": "Ready to process"},
    "walmart": {"status": "Blocking"},
    "bed-bath-beyond": {"status": "Ready to process"},
    "overstock": {"status": "Ready to process"},
    "bisonoffice": {"status": "Ready to process"},
    "english-elm": {"status": "Ready to process"},
    "afa-stores": {"status": "Ready to process"},
    "amazon-seller": {"status": "Not Required"},
    "cymax": {"status": "Ready to process"},
    "homesquare": {"status": "Ready to process", "scraper": "scrapers/custom/cymax/cymax.py"},
    "furniture-cart": {"status": "Not Required"},
    "grayson-living": {"status": "Ready to process"},
    "bedroom-furniture-discounts": {"status": "Ready to process"},
    "unlimited-furniture-group": {"status": "Ready to process"},
    "emma-mason": {"status": "Ready to process"},
    "ashley": {"status": "Not Required"},
    "dining-rooms-outlet": {"status": "Ready to process"},
    "france-and-son": {"status": "Ready to process"},
    "discount-living-rooms": {"status": "Ready to process"},
    "tv-stands-outlet": {"status": "Ready to process"},
    "wayfair": {"status": "Pending"},
    "houzz": {"status": "Ready to process"},
    "luxedecor": {"status": "Ready to process"},
    "target": {"status": "Pending"},
    "home-depot": {"status": "Started"},
    "sears": {"status": "Pending"},
    "perigold": {"status": "Pending"},
    "lowes": {"status": "Pending"},
    "macys": {"status": "Pending"},
    "raymour-flanigan": {"status": "Pending"},
    "bush-furniture": {"status": "Pending"},
    "jcpenney": {"status": "Pending"},
    "grayson-luxury": {"status": "Ready to process"},
    "bloomingdales": {"status": "Ready to process"},
    "kathy-kuo-home": {"status": "Pending"},
    "neiman-marcus": {"status": "Pending"},
    "horchow": {"status": "Pending"},
    "bushfurniture2go": {"status": "Pending"},
    "quill": {"status": "Pending"},
    "staples": {"status": "Pending"},
    "oroa": {"status": "Pending"},
    "us-mattress": {"status": "Pending"},
    "beautyrest": {"status": "Pending"},
    "groupon": {"status": "Pending"},
    "serta": {"status": "Pending"},
    "bellacor": {"status": "Pending"},
    "homethreads": {"status": "Pending"},
    "kohls": {"status": "Pending"},
    "google-shopping": {"status": "Ready to process"}
}

with open("competitors.json", "r") as f:
    data = json.load(f)

for key, update in competitors_update.items():
    if key in data:
        data[key].update(update)
    else:
        # If key is missing, we could add it, but for now let's just log
        print(f"Key {key} not found in competitors.json")

with open("competitors.json", "w") as f:
    json.dump(data, f, indent=4)
