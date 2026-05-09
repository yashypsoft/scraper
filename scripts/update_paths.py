import os
import re

def update_paths():
    # 1. Folders to Scrapers mapping
    custom_folders = [
        "bisonoffice", "blooming-dales", "coleman", "colemanfurniture_scraper", 
        "cymax", "cymax_scraper", "drl", "emma_mason", "fpfc", "graphql", 
        "gshopping", "luxedecor", "ovs-bbb", "unlimited_furniture", "walmart"
    ]
    shopify_folders = ["shopify-scrapper"]

    # 2. Path replacements
    replacements = [
        (r'data/exports', 'data/exports'),
        (r'data/exports', 'data/exports'),
        (r'data/exports/failure_csv', 'data/history/failures'),
        (r'data/exports/failure_csv', 'data/history/failures'),
        (r'match_missing_history\.json', 'data/history/data/history/match_missing_history.json'),
    ]

    # 3. Process all .yml and .py files
    for root, dirs, files in os.walk("."):
        if ".git" in dirs:
            dirs.remove(".git")
        if "venv" in dirs:
            dirs.remove("venv")
        if "data" in dirs:
            dirs.remove("data")

        for file in files:
            if file.endswith((".yml", ".yaml", ".py")):
                path = os.path.join(root, file)
                with open(path, 'r') as f:
                    content = f.read()

                new_content = content
                
                # Update hardcoded output/data paths
                for pattern, repl in replacements:
                    new_content = re.sub(pattern, repl, new_content)

                # Update workflow python command paths
                if file.endswith((".yml", ".yaml")):
                    # Update shopify
                    for folder in shopify_folders:
                        new_content = new_content.replace(f"python {folder}/", f"python scrapers/shopify/{folder}/")
                    # Update custom
                    for folder in custom_folders:
                        new_content = new_content.replace(f"python {folder}/", f"python scrapers/custom/{folder}/")
                        # Handle cases where it might be python3
                        new_content = new_content.replace(f"python3 {folder}/", f"python3 scrapers/custom/{folder}/")

                if new_content != content:
                    print(f"Updating {path}")
                    with open(path, 'w') as f:
                        f.write(new_content)

if __name__ == "__main__":
    update_paths()
