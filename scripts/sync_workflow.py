import json

with open("competitors.json", "r") as f:
    competitors = json.load(f)

competitor_keys = sorted(list(competitors.keys()))

workflow_file = ".github/workflows/run-scraper.yml"
with open(workflow_file, "r") as f:
    lines = f.readlines()

new_lines = []
in_options = False
for line in lines:
    if "options:" in line:
        new_lines.append(line)
        in_options = True
        # Add all keys
        for key in competitor_keys:
            new_lines.append(f"          - {key}\n")
    elif in_options:
        if line.strip().startswith("- "):
            continue
        else:
            in_options = False
            new_lines.append(line)
    else:
        new_lines.append(line)

with open(workflow_file, "w") as f:
    f.writelines(new_lines)
