import requests
from bs4 import BeautifulSoup
import json
import re

url = "https://colemanfurniture.com/cali-pearl-modular-sofa.htm"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

r = requests.get(url, headers=headers)
soup = BeautifulSoup(r.text, 'html.parser')

data = {}

# Product Name
name_elem = soup.find('h1') or soup.find('span', class_='base', itemprop='name')
data['name'] = name_elem.get_text(strip=True) if name_elem else ""

# SKU
sku_elem = soup.find('div', class_='product-sku') or soup.find('div', {'itemprop': 'sku'})
if not sku_elem:
    # Try finding in text
    sku_text = soup.find(string=re.compile(r'SKU:\s*(\S+)'))
    if sku_text:
        match = re.search(r'SKU:\s*(\S+)', sku_text)
        data['sku'] = match.group(1) if match else ""
    else:
        # Fallback to searching all text
        all_text = soup.get_text()
        match = re.search(r'SKU:\s*([A-Z0-9-]+)', all_text)
        data['sku'] = match.group(1) if match else ""
else:
    data['sku'] = sku_elem.get_text(strip=True).replace('SKU:', '').strip()

# Price
price_elem = soup.find('span', class_='price') or soup.find('meta', {'itemprop': 'price'}) or soup.find('span', {'id': 'product-price'})
if price_elem:
    if price_elem.name == 'meta':
        data['price'] = price_elem.get('content')
    else:
        price_text = price_elem.get_text(strip=True)
        # Extract digits and decimal point
        match = re.search(r'([\d,]+\.?\d*)', price_text)
        data['price'] = match.group(1).replace(',', '') if match else ""
else:
    # Try searching for price pattern in all text
    all_text = soup.get_text()
    match = re.search(r'\$([\d,]+\.?\d*)', all_text)
    data['price'] = match.group(1).replace(',', '') if match else ""

# Image
image_elem = soup.find('img', class_='product-image-photo') or soup.find('meta', {'property': 'og:image'})
if image_elem:
    if image_elem.name == 'meta':
        data['image'] = image_elem.get('content')
    else:
        data['image'] = image_elem.get('src')

print(json.dumps(data, indent=2))
