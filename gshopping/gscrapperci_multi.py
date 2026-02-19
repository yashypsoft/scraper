import sys
import json
import random
import ftplib
import os
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import undetected_chromedriver as uc
import csv
import traceback
import pandas as pd
import argparse
import re
from urllib.parse import urlparse, unquote
import concurrent.futures
from threading import Lock
import threading
from queue import Queue
import multiprocessing

# Import the existing captcha solving functions
try:
    from solvecaptcha import solve_recaptcha_audio
except ImportError:
    # If solvecaptcha is not in same directory, try to import from current directory
    import importlib.util
    import sys
    
    # Add current directory to path
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    try:
        from solvecaptcha import solve_recaptcha_audio
    except ImportError:
        print("Warning: solvecaptcha module not found. Captcha solving will be disabled.")
        
        # Define a dummy function if module is not available
        def solve_recaptcha_audio(driver):
            print("Captcha solving module not available. Please install solvecaptcha.")
            return "failed"

# Thread-safe locks for file operations
file_lock = Lock()
stats_lock = Lock()

# Global counters
processed_count = 0
success_count = 0
failed_count = 0
captcha_failed_count = 0

def setup_driver(proxy=None):
    """Setup Chrome driver with optional proxy"""
    time.sleep(2)
    options = uc.ChromeOptions()
    
    # Comment out for local testing to see browser
    # options.add_argument("--headless=new")
    
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--enable-features=NetworkService,NetworkServiceInProcess")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    options.add_argument("--disable-site-isolation-trials")

    # Add proxy if provided
    if proxy:
        options.add_argument(f'--proxy-server={proxy}')

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")

    driver = uc.Chrome(options=options, version_main=144)
    return driver

def detects_recaptcha(driver):
    """Detect if reCAPTCHA is present on the page"""
    try:
        if driver.find_elements(By.CLASS_NAME, "rc-imageselect-challenge"):
            print("Puzzle reCAPTCHA detected!")
            return True
        elif driver.find_elements(By.TAG_NAME, "iframe"):
            for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
                src = iframe.get_attribute("src")
                if src and "recaptcha" in src:
                    print("reCAPTCHA iframe detected!")
                    return True
        else:
            return False
    except Exception as e:
        print(f"Error detecting reCAPTCHA: {e}")
        return False

def handle_captcha(driver, url):
    """Handle captcha if detected with retry logic"""
    max_retries = 1

    for attempt in range(max_retries):
        recaptcha = detects_recaptcha(driver)
        if recaptcha:
            print(f"Attempt {attempt + 1}/{max_retries} to solve captcha...")
            result = solve_recaptcha_audio(driver)
            
            if result == "solved":
                print("Captcha solved successfully!")
                driver.switch_to.default_content()
                return "solved"
            else:
                print(f"Captcha solving attempt {attempt + 1} failed")
                print("All captcha solving attempts failed")
                return "failed"
        else:
            return "no_captcha"
    
    return "failed"

def start_new_driver(search_url, proxy=None):
    """Start a new driver and handle captcha if present"""
    while True:
        driver = setup_driver(proxy)
        driver.get(search_url)
        
        # Handle captcha
        captcha_result = handle_captcha(driver, search_url)
        
        if captcha_result == "solved" or captcha_result == "no_captcha":
            return driver
        else:
            # Captcha solving failed, retry with new driver
            print("Captcha solving failed, retrying with new driver...")
            try:
                driver.quit()
            except:
                pass
            time.sleep(random.uniform(5, 8))

def get_product_options(driver):
    """Extract product variant options from the product panel"""
    scraped_data = {}
    
    try:
        panel = driver.find_element(By.XPATH, "//div[@jsname='Ql2bfc']")
    except NoSuchElementException:
        try:
            panel = driver.find_element(By.XPATH, "//div[@jsname='jzfSje']")
        except NoSuchElementException:
            print("Error: Could not find any product panel container.")
            return json.dumps({}, indent=2)

    # Scrape Swatch-style Filters
    swatch_groups = panel.find_elements(By.XPATH, ".//div[@jsname='iaBacd']")
    
    for group in swatch_groups:
        try:
            title = group.find_element(By.XPATH, ".//span[@class='ZMOBjc']").text
            if not title:
                continue
            
            options = []
            swatches = group.find_elements(By.XPATH, ".//a[@jsname='dbgGYd']")
            for swatch in swatches:
                label = swatch.get_attribute('data-label')
                if label:
                    options.append(label)
            
            if title and options:
                scraped_data[title] = list(dict.fromkeys(options))
                
        except Exception as e:
            print(f"Warning: Could not parse a swatch group. Error: {e}")
            continue

    # Scrape Dropdown-style Filters
    dropdown_groups = panel.find_elements(By.XPATH, ".//div[@data-attrid='variant_picker_chip']")

    for group in dropdown_groups:
        try:
            title_text_element = group.find_element(By.XPATH, ".//div[contains(@class, 'PQev6c')]")
            title_text = title_text_element.get_attribute('textContent').strip()
            
            if ":" in title_text:
                title = title_text.split(":")[0].strip()
            else:
                title = title_text.strip()
                
            if not title:
                continue
                
            options = []
            menu_items = group.find_elements(By.XPATH, ".//g-menu/g-menu-item")
            if menu_items:
                for item in menu_items:
                    try:
                        item_text = item.find_element(By.XPATH, ".//span").get_attribute('textContent').strip()
                        if item_text:
                            options.append(item_text)
                    except NoSuchElementException:
                        continue
            else:
                popup_items = group.find_elements(By.XPATH, ".//g-popup//div[@role='menuitemradio']")
                for item in popup_items:
                    try:
                        item_text = item.find_element(By.XPATH, ".//div[@class='PQev6c']").get_attribute('textContent').strip()
                        if item_text:
                            options.append(item_text)
                    except NoSuchElementException:
                        continue

            if title and options:
                scraped_data[title] = list(dict.fromkeys(options))

        except Exception as e:
            print(f"Warning: Could not parse a dropdown group ('{title}'). Error: {e}")
            continue
    
    return json.dumps(scraped_data, indent=2)

def normalize_url_path_slug(raw_url):
    """Return normalized last path segment (slug), removing query/fragment."""
    try:
        if not raw_url:
            return ""
        cleaned = str(raw_url).strip()
        if not cleaned or cleaned.lower() == "n/a":
            return ""
        if "://" not in cleaned and cleaned.startswith("www."):
            cleaned = f"https://{cleaned}"

        parsed = urlparse(cleaned)
        path = unquote(parsed.path or "").strip()
        path = re.sub(r"/+", "/", path).rstrip("/")
        if not path:
            return ""
        return path.split("/")[-1].strip().lower()
    except:
        return ""

def scrape_product(product_data, proxy=None):
    """Scrape individual product from Google Shopping - to be run in parallel"""
    global processed_count, success_count, failed_count, captcha_failed_count
    
    product_id = product_data['product_id']
    web_id = product_data['web_id']
    keyword = product_data['keyword']
    url = product_data['url']
    osb_url = product_data['osb_url']
    
    thread_id = threading.current_thread().name
    print(f"\n[Thread {thread_id}] Scraping Product ID: {product_id}")
    
    driver = None
    try:
        # Start driver for this product
        driver = start_new_driver(url, proxy)
        
        # Handle captcha before proceeding
        captcha_result = handle_captcha(driver, url)
        if captcha_result == "failed":
            with stats_lock:
                captcha_failed_count += 1
                failed_count += 1
            
            result = {
                'product_id': product_id,
                'web_id': web_id,
                'keyword': keyword,
                'url': url,
                'osb_url': osb_url,
                'last_response': 'Captcha solving failed',
                'status': 'captcha_failed',
                'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'product_url': '',
                'seller': '',
                'product_name': '',
                'cid': '',
                'pid': '',
                'osb_position': 0,
                'osb_id': '',
                'seller_count': 0,
                'competitors': []
            }
            return result, product_data
        
        time.sleep(random.uniform(5, 10))
        
        # Initialize result structure
        result = {
            'product_id': product_id,
            'web_id': web_id,
            'keyword': keyword,
            'url': url,
            'osb_url': osb_url,
            'last_response': '',
            'product_url': '',
            'seller': '',
            'product_name': '',
            'cid': '',
            'pid': '',
            'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'osb_position': 0,
            'osb_id': '',
            'seller_count': 0,
            'status': '',
            'competitors': []
        }
        
        # Try to find product container
        try:
            mains = driver.find_element(By.CLASS_NAME, "dURPMd")
            result['last_response'] = "Product container found"
            result['status'] = "found"
        except Exception as e:
            result['last_response'] = f"Product container not found: {str(e)}"
            result['status'] = "container_not_found"
            with stats_lock:
                failed_count += 1
            return result, product_data
        
        # Find products in container
        products = mains.find_elements(By.CLASS_NAME, 'MtXiu')
        if not products:
            result['last_response'] = "No products found in container"
            result['status'] = "no_products"
            with stats_lock:
                failed_count += 1
            return result, product_data
        
        # Process first matching product
        for product in products:
            try:
                product_name = product.find_element(By.XPATH, ".//div[contains(@class,'gkQHve')]").text
            except:
                product_name = ""
            
            try:
                seller = product.find_element(By.XPATH, ".//span[contains(@class,'WJMUdc')]").text
            except:
                seller = ""
            
            try:
                cid = product.get_attribute('id')
            except:
                cid = ""
            
            # Check for Set keyword mismatch
            if (not "Set" in product_name and "Set" in keyword) or ("Set" in product_name and not "Set" in keyword):
                continue
            
            result.update({
                'product_name': product_name,
                'seller': seller,
                'cid': cid,
                'pid': '',
                'status': 'product_found'
            })
            break
        
        if not result['product_name']:
            result['last_response'] = "No matching product found"
            result['status'] = "no_match"
            with stats_lock:
                failed_count += 1
            return result, product_data
        
        # Click on product if CID exists
        if result['cid']:
            try:
                element = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, f'//div[@id="{result["cid"]}"]'))
                )
                if element:
                    driver.execute_script("arguments[0].scrollIntoView(true);", element)
                    time.sleep(1)
                    element.click()
                    result['last_response'] = "Clicked on product successfully"
                    time.sleep(random.uniform(1, 3))
            except:
                result['last_response'] = "Could not click product element"
        
        # Prefer the stable Google "Share link" URL from the right panel.
        share_url = ""
        try:
            share_button = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//div[contains(@class,'RSNrZe') and @role='button' and @aria-label='Share']"
                ))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", share_button)
            share_button.click()

            share_dialog = WebDriverWait(driver, 8).until(
                EC.visibility_of_element_located((By.XPATH, "//div[@role='dialog' and @aria-label='Share']"))
            )

            try:
                share_input = share_dialog.find_element(By.CSS_SELECTOR, "input[aria-label='Share link'][type='url']")
                share_url = (share_input.get_attribute("value") or "").strip()
            except:
                share_url = ""

            if not share_url:
                try:
                    share_url = share_dialog.find_element(By.CSS_SELECTOR, "div[jsname='tQ9n1c']").text.strip()
                except:
                    share_url = ""

            # Close share dialog so it doesn't block following actions.
            try:
                close_button = share_dialog.find_element(By.CSS_SELECTOR, "[jsname='tqp7ud']")
                close_button.click()
            except:
                try:
                    ActionChains(driver).send_keys(u'\ue00c').perform()  # ESC
                except:
                    pass
        except:
            share_url = ""
        result['product_url'] = share_url or driver.current_url
        
        # Try to get more stores
        i = 0
        while i < 2:
            try:
                more_stores = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'duf-h')]//div[@role='button']"))
                )
                more_stores.click()
                time.sleep(random.uniform(2, 4))
                i += 1
            except:
                break
        
        # Try to find offers grid
        try:
            offers_grid = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@jsname='RSFNod' and @data-attrid='organic_offers_grid']"))
            )
            
            exists = len(driver.find_elements(
                By.XPATH,
                "//div[contains(@class,'iI1aN')]//div[@class='EDblX kjqWgb']"
            )) > 0
            
            if exists > 0:
                product_options = get_product_options(driver)
                result['options'] = product_options
            
            offer_elements = offers_grid.find_elements(By.CLASS_NAME, 'R5K7Cb')
            print(f"[Thread {thread_id}] Found {len(offer_elements)} offers for product {product_id}")
            
            competitors = []
            for seller_html in offer_elements:
                try:
                    store_name = seller_html.find_element(By.CSS_SELECTOR, "div.hP4iBf.gUf0b.uWvFpd").text.strip()
                except:
                    store_name = "N/A"
                
                try:
                    seller_product_name = seller_html.find_element(By.CSS_SELECTOR, "div.Rp8BL").text.strip()
                except:
                    seller_product_name = "N/A"
                
                try:
                    seller_url = seller_html.find_element(By.CSS_SELECTOR, "a.P9159d").get_attribute('href')
                except:
                    seller_url = "N/A"
                
                try:
                    seller_price_element = seller_html.find_element(By.CSS_SELECTOR, "div.QcEgce span[aria-hidden='true']")
                    seller_price = seller_price_element.text.strip()
                except:
                    try:
                        seller_price_element = seller_html.find_element(By.CSS_SELECTOR, "div.GBgquf span")
                        seller_price = seller_price_element.text.strip()
                    except:
                        seller_price = "N/A"
                
                competitor_data = {
                    'product_id': product_id,
                    'seller': store_name,
                    'seller_product_name': seller_product_name,
                    'seller_url': seller_url,
                    'seller_price': seller_price,
                    'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                competitors.append(competitor_data)
                result['competitors'].append(competitor_data)
            
            # Calculate OSB position
            search_seller = '1StopBedrooms'
            sellers = [c['seller'] for c in competitors]
            osb_position = 0
            seller_count = len(sellers)
            osb_id = ''
            osb_url_match = False
            
            if search_seller in sellers:
                osb_position = sellers.index(search_seller) + 1
                for competitor in competitors:
                    if competitor['seller'] == search_seller:
                        seller_slug = normalize_url_path_slug(competitor.get('seller_url', ''))
                        osb_id = seller_slug
                        target_slug = normalize_url_path_slug(osb_url)
                        if seller_slug and target_slug:
                            osb_url_match = seller_slug == target_slug
                        break
            
            result.update({
                'osb_position': osb_position,
                'seller_count': seller_count,
                'osb_id': osb_id,
                'status': 'completed',
                'last_response': f'Completed - OSB Position: {osb_position}, Total Sellers: {seller_count}, OSB URL Match: {"Yes" if osb_url_match else "No"}'
            })
            
            with stats_lock:
                success_count += 1
            
        except Exception as e:
            result['status'] = 'no_offers_found'
            result['last_response'] = f'No offers found: {str(e)}'
            with stats_lock:
                failed_count += 1
        
        with stats_lock:
            processed_count += 1
            print(f"\n[Thread {thread_id}] Progress: {processed_count} processed, {success_count} successful, {failed_count} failed, {captcha_failed_count} captcha failed")
        
        return result, product_data
        
    except Exception as e:
        print(f"[Thread {thread_id}] Error scraping product {product_id}: {str(e)}")
        traceback.print_exc()
        with stats_lock:
            failed_count += 1
        
        result = {
            'product_id': product_id,
            'web_id': web_id,
            'keyword': keyword,
            'url': url,
            'osb_url': osb_url,
            'last_response': f'Error: {str(e)}',
            'status': 'error',
            'last_fetched_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'product_url': '',
            'seller': '',
            'product_name': '',
            'cid': '',
            'pid': '',
            'osb_position': 0,
            'osb_id': '',
            'seller_count': 0,
            'competitors': []
        }
        return result, product_data
        
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def download_csv_from_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, remote_filename, local_filename):
    """Download CSV file from FTP"""
    try:
        print(f"Downloading {remote_filename} from FTP...")
        
        ftp = ftplib.FTP()
        ftp.connect(ftp_host, int(os.getenv("FTP_PORT", 21)))
        ftp.login(ftp_user, ftp_pass)
        ftp.set_pasv(True)
        
        if ftp_path and ftp_path != '/':
            try:
                ftp.cwd(ftp_path)
            except:
                print(f"Error: Could not change to directory {ftp_path}")
                return None
        
        with open(local_filename, 'wb') as f:
            ftp.retrbinary(f'RETR {remote_filename}', f.write)
        
        ftp.quit()
        print(f"✓ Downloaded {remote_filename} to {local_filename}")
        return local_filename
        
    except Exception as e:
        print(f"Error downloading from FTP: {str(e)}")
        return None

def upload_to_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, local_file, remote_filename):
    """Upload file to FTP server"""
    try:
        print(f"Uploading {remote_filename} to FTP...")
        
        ftp = ftplib.FTP()
        ftp.connect(ftp_host, 21)
        ftp.login(ftp_user, ftp_pass)
        ftp.set_pasv(True)
        
        if ftp_path and ftp_path != '/':
            try:
                ftp.cwd(ftp_path)
            except:
                dirs = ftp_path.strip('/').split('/')
                current_path = ''
                for dir in dirs:
                    current_path += '/' + dir
                    try:
                        ftp.cwd(current_path)
                    except:
                        ftp.mkd(current_path)
                        ftp.cwd(current_path)
        
        with open(local_file, 'rb') as f:
            ftp.storbinary(f'STOR {remote_filename}', f)
        
        ftp.quit()
        print(f"✓ Uploaded {remote_filename} to FTP")
        return True
        
    except Exception as e:
        print(f"Error uploading to FTP: {str(e)}")
        return False

def split_csv(input_csv, output_dir, chunk_id, total_chunks):
    """Split CSV into chunks and return specific chunk"""
    try:
        df = pd.read_csv(input_csv)
        
        if df.empty:
            print("CSV file is empty")
            return None
        
        total_rows = len(df)
        rows_per_chunk = total_rows // total_chunks
        
        start_idx = (chunk_id - 1) * rows_per_chunk
        end_idx = chunk_id * rows_per_chunk if chunk_id < total_chunks else total_rows
        
        chunk_df = df.iloc[start_idx:end_idx]
        
        os.makedirs(output_dir, exist_ok=True)
        chunk_filename = f"chunk_{chunk_id}.csv"
        chunk_path = os.path.join(output_dir, chunk_filename)
        
        chunk_df.to_csv(chunk_path, index=False)
        
        print(f"Chunk {chunk_id}: Rows {start_idx+1} to {end_idx} ({len(chunk_df)} rows)")
        return chunk_path
        
    except Exception as e:
        print(f"Error splitting CSV: {str(e)}")
        return None

def process_chunk_parallel(chunk_file, chunk_id, total_chunks, max_workers=None):
    """Process a chunk of products in parallel"""
    global processed_count, success_count, failed_count, captcha_failed_count
    
    try:
        # Reset counters
        processed_count = 0
        success_count = 0
        failed_count = 0
        captcha_failed_count = 0
        
        # Get FTP credentials from environment
        ftp_host = os.getenv('FTP_HOST')
        ftp_user = os.getenv('FTP_USER')
        ftp_pass = os.getenv('FTP_PASS')
        ftp_path = os.getenv('FTP_PATH', '/scrap/')
        
        if not all([ftp_host, ftp_user, ftp_pass]):
            print("Error: FTP credentials not found")
            return False
        
        # Read chunk file
        df = pd.read_csv(chunk_file)
        print(f"\nProcessing {len(df)} products from chunk {chunk_id}")
        
        # Determine number of workers (default: CPU count * 2)
        if max_workers is None:
            max_workers = min(multiprocessing.cpu_count() * 2, len(df))
            # Cap at reasonable number to avoid overwhelming the system
            max_workers = min(max_workers, 5)
        
        print(f"Using {max_workers} parallel workers")
        
        # Prepare product data for parallel processing
        products_to_process = []
        for _, row in df.iterrows():
            product_data = {
                'product_id': row['product_id'],
                'web_id': row['web_id'],
                'keyword': row['keyword'],
                'url': row['url'],
                'osb_url': row['osb_url']
            }
            products_to_process.append(product_data)
        
        # Initialize results containers
        all_product_results = []
        all_seller_results = []
        remaining_results = []
        
        # Process products in parallel
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="Scraper") as executor:
            # Submit all tasks
            future_to_product = {
                executor.submit(scrape_product, product_data): product_data 
                for product_data in products_to_process
            }
            
            # Process completed futures as they come in
            for future in concurrent.futures.as_completed(future_to_product):
                product_data = future_to_product[future]
                try:
                    product_result, original_row = future.result(timeout=300)  # 5 minute timeout
                    
                    # Add to results
                    with file_lock:
                        all_product_results.append(product_result)
                        all_seller_results.extend(product_result['competitors'])
                        
                        # Check if we need to retry this product
                        if product_result.get('status') == 'captcha_failed':
                            remaining_results.append(original_row)
                    
                except concurrent.futures.TimeoutError:
                    print(f"Timeout processing product {product_data['product_id']}")
                    with file_lock:
                        failed_count += 1
                        # Add to remaining for retry
                        remaining_results.append(product_data)
                except Exception as e:
                    print(f"Error processing product {product_data['product_id']}: {str(e)}")
                    with file_lock:
                        failed_count += 1
        
        elapsed_time = time.time() - start_time
        print(f"\nParallel processing completed in {elapsed_time:.2f} seconds")
        
        # Create CSV 1: Product Information
        csv1_data = []
        for result in all_product_results:
            csv1_row = {
                'product_id': result.get('product_id', ''),
                'web_id': result.get('web_id', ''),
                'keyword': result.get('keyword', ''),
                'url': result.get('url', ''),
                'osb_url': result.get('osb_url', ''),
                'last_response': result.get('last_response', ''),
                'product_url': result.get('product_url', ''),
                'seller': result.get('seller', ''),
                'product_name': result.get('product_name', ''),
                'cid': result.get('cid', ''),
                'pid': result.get('pid', ''),
                'last_fetched_date': result.get('last_fetched_date', ''),
                'osb_position': result.get('osb_position', 0),
                'osb_id': result.get('osb_id', ''),
                'seller_count': result.get('seller_count', 0),
                'status': result.get('status', 'error')
            }

            csv1_data.append(csv1_row)
        
        # Create CSV 2: Seller Information
        csv2_data = []
        for seller in all_seller_results:
            csv2_row = {
                'product_id': seller['product_id'],
                'seller': seller['seller'],
                'seller_product_name': seller['seller_product_name'],
                'seller_url': seller['seller_url'],
                'seller_price': seller['seller_price'],
                'last_fetched_date': seller['last_fetched_date']
            }
            csv2_data.append(csv2_row)
        
        # Save CSV files locally
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        
        csv1_filename = f"product_info_chunk{chunk_id}_{timestamp}.csv"
        csv2_filename = f"seller_info_chunk{chunk_id}_{timestamp}.csv"
        csv3_filename = f"gshopping_remaining_chunk{chunk_id}_{timestamp}.csv"
        
        csv1_path = os.path.join(output_dir, csv1_filename)
        csv2_path = os.path.join(output_dir, csv2_filename)
        csv3_path = os.path.join(output_dir, csv3_filename)
        
        if csv1_data:
            pd.DataFrame(csv1_data).to_csv(csv1_path, index=False)
            print(f"✓ Saved product info: {csv1_filename} ({len(csv1_data)} records)")
        
        if csv2_data:
            pd.DataFrame(csv2_data).to_csv(csv2_path, index=False)
            print(f"✓ Saved seller info: {csv2_filename} ({len(csv2_data)} records)")

        if remaining_results:
            pd.DataFrame(remaining_results).to_csv(csv3_path, index=False)
            print(f"✓ Saved remaining rows: {csv3_filename} ({len(remaining_results)} records)")
        
        # Upload to FTP (commented out to avoid unnecessary FTP usage during testing)
        # if csv1_data:
        #     upload_to_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, csv1_path, csv1_filename)
        
        # if csv2_data:
        #     upload_to_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, csv2_path, csv2_filename)
        
        print(f"\n✓ Chunk {chunk_id} processing completed")
        print(f"  Total: {processed_count} | Success: {success_count} | Failed: {failed_count} | Captcha Failed: {captcha_failed_count}")
        return True
        
    except Exception as e:
        print(f"Error processing chunk {chunk_id}: {str(e)}")
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description='Google Shopping Scraper with Parallel Processing')
    parser.add_argument('--chunk-id', type=int, required=True, help='Chunk ID (1-based)')
    parser.add_argument('--total-chunks', type=int, required=True, help='Total number of chunks')
    parser.add_argument('--input-file', type=str, required=True, help='Input CSV filename on FTP')
    parser.add_argument('--workers', type=int, default=None, help='Number of parallel workers (default: auto)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Google Shopping Scraper with Parallel Processing")
    print(f"Chunk: {args.chunk_id} of {args.total_chunks}")
    print(f"Input file: {args.input_file}")
    print(f"Parallel workers: {args.workers if args.workers else 'auto'}")
    print("=" * 60)
    
    # Get FTP credentials
    ftp_host = os.getenv('FTP_HOST')
    ftp_user = os.getenv('FTP_USER')
    ftp_pass = os.getenv('FTP_PASS')
    ftp_path = os.getenv('FTP_PATH', '/scrap/')
    
    if not all([ftp_host, ftp_user, ftp_pass]):
        print("Error: FTP credentials not found in environment variables")
        print("Please set FTP_HOST, FTP_USER, FTP_PASS environment variables")
        sys.exit(1)
    
    # Download input CSV from FTP
    input_csv = 'input.csv'
    if not download_csv_from_ftp(ftp_host, ftp_user, ftp_pass, ftp_path, args.input_file, input_csv):
        print("Failed to download input CSV")
        sys.exit(1)
    
    # Split CSV and get our chunk
    chunk_file = split_csv(input_csv, 'chunks', args.chunk_id, args.total_chunks)
    if not chunk_file:
        print("Failed to split CSV")
        sys.exit(1)
    
    # Process the chunk in parallel
    success = process_chunk_parallel(chunk_file, args.chunk_id, args.total_chunks, args.workers)
    
    # Clean up
    try:
        os.remove(input_csv)
        os.remove(chunk_file)
        import shutil
        shutil.rmtree('chunks', ignore_errors=True)
    except:
        pass
    
    if success:
        print("\n✓ Processing completed successfully")
        sys.exit(0)
    else:
        print("\n✗ Processing failed")
        sys.exit(1)

if __name__ == "__main__":
    main()