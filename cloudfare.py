import requests
import re
import pandas as pd
import cloudscraper

def extract_all_sitemap_urls(base_url):
    """Extract all URLs from sitemap and nested sitemaps"""
    scraper = cloudscraper.create_scraper()
    
    def get_urls_from_sitemap(url):
        try:
            print(f"Fetching: {url}")
            response = scraper.get(url, timeout=30)
            content = response.text
            
            # Find all URLs in sitemap
            urls = re.findall(r'<loc>(https?://[^<]+)</loc>', content)
            
            # Check if there are nested sitemaps
            nested_sitemaps = [u for u in urls if 'sitemap' in u.lower() and url != u]
            regular_urls = [u for u in urls if u not in nested_sitemaps]
            
            all_urls = regular_urls
            
            # Recursively fetch nested sitemaps
            for sitemap in nested_sitemaps:
                nested_urls = get_urls_from_sitemap(sitemap)
                all_urls.extend(nested_urls)
            
            return all_urls
            
        except Exception as e:
            print(f"Error with {url}: {e}")
            return []
    
    return get_urls_from_sitemap(base_url)

# Usage
sitemap_url = "https://api.overstock.com/sitemaps/overstock-v3/us/sitemap.xml"
all_urls = extract_all_sitemap_urls(sitemap_url)

print(f"\nTotal URLs found: {len(all_urls)}")

# Categorize URLs
categories = {
    'Products': [u for u in all_urls if '/product' in u.lower()],
    'Categories': [u for u in all_urls if '/collection' in u.lower() or '/category' in u.lower()],
    'Pages': [u for u in all_urls if '/page' in u.lower() or '/pages' in u.lower()],
    'Blog': [u for u in all_urls if '/blog' in u.lower() or '/article' in u.lower()],
    'Other': [u for u in all_urls if all(key not in u.lower() for key in ['/product', '/collection', '/category', '/page', '/blog', '/article'])]
}

for category, urls in categories.items():
    print(f"{category}: {len(urls)} URLs")

# Save to CSV
df = pd.DataFrame({
    'URL': all_urls,
    'Type': ['Product' if '/product' in u.lower() else 
             'Category' if '/collection' in u.lower() or '/category' in u.lower() else
             'Page' if '/page' in u.lower() else
             'Blog' if '/blog' in u.lower() else
             'Other' for u in all_urls]
})

df.to_csv('afa_stores_sitemap_urls.csv', index=False)
print("\n✅ URLs saved to 'afa_stores_sitemap_urls.csv'")