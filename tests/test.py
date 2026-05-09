import requests

BASE = "http://127.0.0.1:11434/api/generate"

data = {
    "model": "kimi-k2.5:cloud",       # your pulled model name
    "prompt": "Hello from Python!",
    "stream": False
}

resp = requests.post(BASE, json=data, timeout=30)
resp.raise_for_status()

result = resp.json()
print(result["response"])