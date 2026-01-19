import requests
from bs4 import BeautifulSoup
import csv
import random
import time

# Auto-incrementing product_id for this example
global_product_id = 1

# Function to extract text from HTML element safely
def extract_text_or_none(element, default=None):
    return element.get_text(strip=True) if element else default

# Function to extract attribute content safely
def extract_attr_or_none(element, attr, default=None):
    return element[attr] if element and element.has_attr(attr) else default

# Function to get data from the product page
def scrape_product(url):
    try:
        global global_product_id

        # Request the page content
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract product information
        product_data = []
        
        # Main product details
        name = extract_text_or_none(soup.find('div', class_='product-name').find('h1', {'itemprop': 'name'}))
        sku = extract_attr_or_none(soup.find('meta', {'itemprop': 'sku'}), 'content')
        brand = extract_text_or_none(soup.select_one('p.manufacturer a:nth-of-type(2)'))
        collection = extract_text_or_none(soup.select_one('p.manufacturer a:nth-of-type(1)'))
        image = extract_attr_or_none(soup.find('meta', {'itemprop': 'image'}), 'content')

        # Extract the main product price (for bundle)
        price_element = soup.find('div', class_='price-box').find('span', class_='price')
        current_price = extract_text_or_none(price_element).replace('price-old', '').strip() if price_element else "N/A"
        current_price = current_price.replace('$','')

        product_type = 'bundle' if soup.find('div', class_='product-options') else 'simple'
        
        # Add the main product row
        product_data.append({
            'product_id': global_product_id,
            'main_product_id': global_product_id,  # Main product ID is same for bundle
            'product_type': product_type,
            'url': url,
            'name': name,
            'sku': sku,
            'brand': brand,
            'collection': collection,
            'image': image,
            'price': current_price  # Use current_price here
        })

        # Increment the product_id for the next product
        main_product_id = global_product_id
        global_product_id += 1

        # If product is a bundle, extract set simple products
        product_options = soup.find('div', class_='product-options')
        if product_options:
            simple_products = product_options.find_all('li', class_='item')

            for simple in simple_products:
                # Extract details for multiple products in the same li
                product_labels = simple.find_all('span', class_='select-label')
                if len(product_labels) > 0:
                    price_divs = simple.find('div', class_='price-holder').find_all('div', class_='price')

                    # Collect prices in a list to associate them with the correct product
                    prices = [extract_text_or_none(price_div).split('$')[-1].strip() for price_div in price_divs]
                    
                    image_links = simple.find_all('a', class_='lightbox-bundle-group')
                    image_urls = [extract_attr_or_none(link, 'href') for link in image_links]
                    
                    url_links = simple.find_all('a', title="Go to product")
                    product_urls = [extract_attr_or_none(link, 'href') for link in url_links]

                    for index, label in enumerate(product_labels):
                        simple_name = extract_text_or_none(label.find('strong', class_='title'))
                        simple_sku_element = label.find('span')
                        simple_sku = simple_sku_element.text.split('ID: ')[-1].split()[0] if simple_sku_element else "N/A"
                        simple_current_price = prices[index] if index < len(prices) else "N/A"
                        simple_image_url = image_urls[index] if index < len(image_urls) else "N/A"
                        simple_product_url = product_urls[index] if index < len(product_urls) else "N/A"

                        # Add each set-simple product row
                        product_data.append({
                            'product_id': global_product_id,
                            'main_product_id': main_product_id,
                            'product_type': 'set-simple',
                            'url': simple_product_url,
                            'name': simple_name,
                            'sku': simple_sku,
                            'brand': brand,
                            'collection': collection,
                            'image': simple_image_url,
                            'price': simple_current_price,
                        })

                        global_product_id += 1
                else :
                    controlClass = simple.find('div', class_='controls')
                    simple_product_url = extract_attr_or_none(controlClass.find('a'),'href')

                    simple_sku_element = controlClass.find('span',class_='label')
                    simple_sku = simple_sku_element.text.split('ID: ')[-1].split()[0] if simple_sku_element else "N/A"

                    simple_name = simple_product_url = extract_text_or_none(controlClass.find('a'))

                    price_element = simple.find('div', class_='price')
                    simple_current_price = price_element.text.strip().split()[-1] if price_element else "N/A"
                    simple_current_price = simple_current_price.replace('$','')

                    imageBlock = simple.find('div',class_='product-img')
                    simple_image_url = extract_attr_or_none(imageBlock.find('a', class_='lightbox-bundle-group'),'href')

                    product_data.append({
                        'product_id': global_product_id,
                        'main_product_id': main_product_id,
                        'product_type': 'set-simple',
                        'url': simple_product_url,
                        'name': simple_name,
                        'sku': simple_sku,
                        'brand': brand,
                        'collection': collection,
                        'image': simple_image_url,
                        'price': simple_current_price,
                    })
                    global_product_id += 1

        return product_data
    except:
        return[]

# List of URLs to scrape from CSV file
input_csv = "input_urls.csv"
output_csv = "furniture_products.csv"

# Read URLs from the input CSV
with open(input_csv, 'r') as file:
    urls = [row[0] for row in csv.reader(file)]

# Save data in chunks to CSV after scraping each URL
csv_columns = ['product_id', 'main_product_id', 'product_type', 'url', 'name', 'sku', 'brand', 'collection', 'image', 'price']

try:
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()

        request_counter = 0
        i = 0
        for url in urls:
            i = i + 1
            product_data = scrape_product(url)

            # Write product data after scraping each URL
            print(product_data)
            print(i)
            if len(product_data) == 0:
                continue
            for data in product_data:
                writer.writerow(data)
            # Increment request counter and apply random timeout
            request_counter += 1
            if request_counter % random.randint(30, 50) == 0:  # Random pause after 5-10 requests
                time_to_sleep = random.uniform(3, 7)  # Random sleep duration between 5 to 15 seconds
                print(f"Sleeping for {time_to_sleep:.2f} seconds to avoid being blocked.")
                time.sleep(time_to_sleep)
except IOError:
    print("I/O error")
