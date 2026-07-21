import os
import sys
import time
import zipfile
import traceback
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd
import pymysql
import numpy as np

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def get_site_display(competitor_name, seller_name, seller_url, base_url):
    domain = ''
    for url in (seller_url, base_url):
        if url and isinstance(url, str) and url.strip():
            try:
                parsed = urlparse(url.strip())
                netloc = parsed.netloc or parsed.path.split('/')[0]
                if netloc:
                    domain = netloc.lower().replace('www.', '')
                    break
            except Exception:
                pass
                
    if not domain and competitor_name:
        domain = str(competitor_name).lower().replace('www.', '')
        
    s_name = str(seller_name).strip() if seller_name else ''
    
    if domain and s_name:
        if domain.lower() == s_name.lower():
            return s_name
        else:
            return f"{domain} — {s_name}"
    return s_name or domain or ''

def format_last_update_cycle(dt):
    if dt is None or pd.isna(dt):
        dt = datetime.now()
    # Format: 19 May 2026 00:30 (GMT ZZZZ)
    return dt.strftime("%d %b %Y %H:%M (GMT ZZZZ)")

def get_site_display_and_is_me_batch(comp_list, sel_list, s_url_list, b_url_list):
    site_displays = []
    is_me_list = []
    domain_cache = {}
    
    for comp, sel, s_url, b_url in zip(comp_list, sel_list, s_url_list, b_url_list):
        domain = ''
        for url in (s_url, b_url):
            if url:
                url_str = url.strip()
                if url_str:
                    if url_str in domain_cache:
                        domain = domain_cache[url_str]
                        break
                    try:
                        parsed = urlparse(url_str)
                        netloc = parsed.netloc or parsed.path.split('/')[0]
                        if netloc:
                            domain = netloc.lower().replace('www.', '')
                            domain_cache[url_str] = domain
                            break
                    except Exception:
                        pass
        
        if not domain and comp:
            comp_str = str(comp).strip().lower().replace('www.', '')
            domain = comp_str
            
        s_name = str(sel).strip() if sel else ''
        
        if domain and s_name:
            if domain.lower() == s_name.lower():
                display = s_name
            else:
                display = f"{domain} — {s_name}"
        else:
            display = s_name or domain or ''
            
        site_displays.append(display)
        
        dom_for_chk = display.split(' — ')[0].lower() if ' — ' in display else display.lower()
        is_me = (s_name.lower() == '1stopbedrooms' or dom_for_chk == '1stopbedrooms.com')
        is_me_list.append(is_me)
        
    return site_displays, is_me_list

def upload_to_oracle_sftp(local_file, remote_filename):
    ssh_key_path = os.environ.get("ORACLE_SFTP_KEY_PATH")
    private_key_content = os.environ.get("ORACLE_SFTP_PRIVATE_KEY")
    remote_host = os.environ.get("ORACLE_SFTP_HOST")
    remote_user = os.environ.get("ORACLE_SFTP_USER")
    remote_dir = os.environ.get("ORACLE_SFTP_REMOTE_DIR")
    
    # If the key path variable itself contains the actual key contents, redirect it
    if ssh_key_path and ssh_key_path.strip().startswith("-----BEGIN"):
        private_key_content = ssh_key_path
        ssh_key_path = None
        
    if not (ssh_key_path or private_key_content) or not all([remote_host, remote_user, remote_dir]):
        print("❌ Failed to upload to Oracle server: Missing SFTP configuration in environment variables.")
        return False
        
    temp_key_file = None
    try:
        if private_key_content:
            import tempfile
            fd, temp_key_file = tempfile.mkstemp(prefix="oracle_ssh_")
            with os.fdopen(fd, 'w') as f:
                f.write(private_key_content.strip() + "\n")
            os.chmod(temp_key_file, 0o600)
            ssh_key_path = temp_key_file
        else:
            ssh_key_path = os.path.expanduser(ssh_key_path)
            
        print(f"Uploading {local_file} to Oracle Server {remote_dir}...")
        
        cmd = [
            "scp",
            "-i", ssh_key_path,
            "-o", "StrictHostKeyChecking=no",
            local_file,
            f"{remote_user}@{remote_host}:{remote_dir}/{remote_filename}"
        ]
        
        import subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print("✓ Successfully uploaded to Oracle server over SCP!")
        
        # Expose and print the HTTP URL
        web_dir = "/var/www/html/"
        web_path = ""
        if remote_dir.startswith(web_dir):
            web_path = remote_dir[len(web_dir):].rstrip('/')
            
        http_url = f"http://{remote_host}{web_path}/{remote_filename}"
        print(f"🔗 File is exposed at: Server")
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to upload to Oracle server: {e.stderr}")
        return False
    except Exception as e:
        print(f"Error executing scp command: {e}")
        return False
    finally:
        if temp_key_file and os.path.exists(temp_key_file):
            try:
                os.remove(temp_key_file)
            except Exception:
                pass

def get_connection():
    mysql_host = os.environ.get("MYSQL_HOST")
    mysql_port = os.environ.get("MYSQL_PORT", "3306")
    try:
        mysql_port = int(mysql_port)
    except ValueError:
        mysql_port = 3306
    mysql_user = os.environ.get("MYSQL_USER")
    mysql_pass = os.environ.get("MYSQL_PASS")
    mysql_db = os.environ.get("MYSQL_DB")
    conn = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_pass,
        database=mysql_db
    )
    conn.autocommit(True)
    return conn

def safe_read_sql(sql, params, conn_holder, max_retries=5):
    for attempt in range(max_retries):
        try:
            if conn_holder[0] is None:
                print("Connecting to MySQL...")
                conn_holder[0] = get_connection()
            else:
                try:
                    conn_holder[0].ping()
                except Exception:
                    print("Re-connecting to MySQL...")
                    conn_holder[0] = get_connection()
            with conn_holder[0].cursor() as cursor:
                cursor.execute(sql, params)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
        except Exception as e:
            print(f"Database error on attempt {attempt+1}/{max_retries}: {e}")
            if attempt == max_retries - 1:
                raise
            try:
                if conn_holder[0]:
                    conn_holder[0].close()
            except Exception:
                pass
            conn_holder[0] = None
            time.sleep(2 ** attempt)

def main():
    if os.environ.get("ORACLE_SFTP_UPLOAD") != "1":
        print("Skipping export generation and upload because ORACLE_SFTP_UPLOAD is not set to '1'.")
        sys.exit(0)

    print("Connecting to MySQL...")
    conn_holder = [None]
    try:
        conn_holder[0] = get_connection()
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        sys.exit(1)

    print("Fetching active product IDs from database...")
    try:
        product_ids = []
        for attempt in range(3):
            cursor = None
            try:
                if conn_holder[0] is None:
                    conn_holder[0] = get_connection()
                else:
                    try:
                        conn_holder[0].ping()
                    except:
                        conn_holder[0] = get_connection()
                cursor = conn_holder[0].cursor()
                cursor.execute(
                    """
                    SELECT p.product_id 
                    FROM osb_products p
                    JOIN google_shopping_results r ON p.product_id = r.product_id
                    WHERE p.status = 1 
                      AND p.scraping_status = 'completed'
                    """
                )
                product_ids = [row[0] for row in cursor.fetchall()]
                cursor.close()
                break
            except Exception as e:
                print(f"Failed to fetch product IDs on attempt {attempt+1}/3: {e}")
                if cursor:
                    try:
                        cursor.close()
                    except:
                        pass
                if attempt == 2:
                    raise
                if conn_holder[0]:
                    try:
                        conn_holder[0].close()
                    except:
                        pass
                conn_holder[0] = None
                time.sleep(2 ** attempt)
        
        print(f"Total matching product IDs: {len(product_ids)}")
        
        print("Fetching products and scraped results in single-roundtrip queries...")
        products_df = safe_read_sql(
            """
            SELECT 
                p.product_id, 
                p.name, 
                p.gtin, 
                p.brand, 
                p.product_type AS category, 
                p.keyword, 
                p.url, 
                p.osb_url, 
                p.price, 
                p.margin, 
                p.scraping_status 
            FROM osb_products p
            JOIN google_shopping_results r ON p.product_id = r.product_id
            WHERE p.status = 1 
              AND p.scraping_status = 'completed'
            """,
            params=None,
            conn_holder=conn_holder
        )
        
        results_df = safe_read_sql(
            """
            SELECT 
                r.product_id, 
                r.google_title, 
                r.seller_count, 
                r.osb_position, 
                r.updated_at, 
                r.google_seller_page_url, 
                r.osb_url_match 
            FROM google_shopping_results r
            JOIN osb_products p ON p.product_id = r.product_id
            WHERE p.status = 1 
              AND p.scraping_status = 'completed'
            """,
            params=None,
            conn_holder=conn_holder
        )

        CHUNK_SIZE = 1000
        sellers_frames = []
        
        total_chunks = (len(product_ids) + CHUNK_SIZE - 1) // CHUNK_SIZE
        for idx, offset in enumerate(range(0, len(product_ids), CHUNK_SIZE), 1):
            chunk_ids = product_ids[offset:offset+CHUNK_SIZE]
            print(f"Fetching sellers chunk {idx} of {total_chunks} ({len(chunk_ids)} products)...")
            
            placeholders = ", ".join(["%s"] * len(chunk_ids))
            s_df = safe_read_sql(
                f"""
                SELECT 
                    s.product_id, 
                    COALESCE(s.seller_name, '') AS seller_name, 
                    s.price AS seller_price, 
                    COALESCE(s.seller_url, '') AS seller_url, 
                    COALESCE(s.stock_status, 'In Stock') AS stock_status,
                    COALESCE(s.site_display, '') AS site_display,
                    COALESCE(s.is_me, FALSE) AS is_me
                FROM google_shopping_sellers s
                WHERE s.product_id IN ({placeholders})
                """,
                params=tuple(chunk_ids),
                conn_holder=conn_holder
            )
            sellers_frames.append(s_df)
            
        seller_cols = ['product_id', 'seller_name', 'seller_price', 'seller_url', 'stock_status', 'site_display', 'is_me']
        sellers_df = pd.concat(sellers_frames, ignore_index=True) if sellers_frames else pd.DataFrame(columns=seller_cols)
        
    except Exception as e:
        print(f"Failed to fetch data from database: {e}")
        traceback.print_exc()
        try:
            if conn_holder[0]:
                conn_holder[0].close()
        except:
            pass
        sys.exit(1)
    finally:
        try:
            if conn_holder[0]:
                conn_holder[0].close()
        except:
            pass

    print(f"Processing data: {len(products_df)} products, {len(results_df)} scraped results, {len(sellers_df)} sellers.")

    now_dt = datetime.now()
    
    # 1. Prepare and clean sellers_df in-place (saves huge memory allocations)
    sellers_clean = sellers_df
    sellers_clean['seller_price'] = pd.to_numeric(sellers_clean['seller_price'], errors='coerce')
    sellers_clean['is_me'] = sellers_clean['is_me'].astype(bool)
    
    # 2. Compute my_price per product
    me_sellers = sellers_clean[sellers_clean['is_me']]
    me_prices = me_sellers.dropna(subset=['seller_price']).groupby('product_id')['seller_price'].first()
    
    products = products_df.copy()
    products['my_price'] = products['product_id'].map(me_prices)
    products['my_price'] = pd.to_numeric(products['my_price'], errors='coerce').fillna(0.00)
    
    # 3. Compute product cost
    products['margin'] = pd.to_numeric(products['margin'], errors='coerce')
    products['my_product_cost'] = 0.00
    valid_cost_mask = (products['my_price'] > 0) & (products['margin'].notna())
    products.loc[valid_cost_mask, 'my_product_cost'] = (
        products.loc[valid_cost_mask, 'my_price'] / (products.loc[valid_cost_mask, 'margin'] / 100.0 + 1.0)
    ).round(2)
    
    # 4. Compute competitor pricing statistics
    comp_sellers = sellers_clean[~sellers_clean['is_me'] & sellers_clean['seller_price'].notna()]
    comp_price_stats = comp_sellers.groupby('product_id')['seller_price'].agg(['count', 'min', 'max', 'sum'])
    
    products = products.merge(
        comp_price_stats.rename(columns={
            'count': 'comp_count',
            'min': 'comp_min',
            'max': 'comp_max',
            'sum': 'comp_sum'
        }),
        on='product_id',
        how='left'
    )
    products['comp_count'] = products['comp_count'].fillna(0).astype(int)
    products['comp_sum'] = products['comp_sum'].fillna(0.0)
    
    # 5. Compute min_price, max_price, avg_price
    has_my_price = products['my_price'] > 0
    
    # Min price
    products['min_price'] = 0.00
    products.loc[has_my_price & (products['comp_count'] > 0), 'min_price'] = np.minimum(
        products.loc[has_my_price & (products['comp_count'] > 0), 'comp_min'],
        products.loc[has_my_price & (products['comp_count'] > 0), 'my_price']
    )
    products.loc[has_my_price & (products['comp_count'] == 0), 'min_price'] = products.loc[has_my_price & (products['comp_count'] == 0), 'my_price']
    products.loc[~has_my_price & (products['comp_count'] > 0), 'min_price'] = products.loc[~has_my_price & (products['comp_count'] > 0), 'comp_min']
    
    # Max price
    products['max_price'] = 0.00
    products.loc[has_my_price & (products['comp_count'] > 0), 'max_price'] = np.maximum(
        products.loc[has_my_price & (products['comp_count'] > 0), 'comp_max'],
        products.loc[has_my_price & (products['comp_count'] > 0), 'my_price']
    )
    products.loc[has_my_price & (products['comp_count'] == 0), 'max_price'] = products.loc[has_my_price & (products['comp_count'] == 0), 'my_price']
    products.loc[~has_my_price & (products['comp_count'] > 0), 'max_price'] = products.loc[~has_my_price & (products['comp_count'] > 0), 'comp_max']
    
    # Average price
    products['avg_price'] = 0.00
    products.loc[has_my_price, 'avg_price'] = (
        (products.loc[has_my_price, 'comp_sum'] + products.loc[has_my_price, 'my_price']) /
        (products.loc[has_my_price, 'comp_count'] + 1)
    )
    comp_only_mask = ~has_my_price & (products['comp_count'] > 0)
    products.loc[comp_only_mask, 'avg_price'] = (
        products.loc[comp_only_mask, 'comp_sum'] / products.loc[comp_only_mask, 'comp_count']
    )
    products['avg_price'] = products['avg_price'].round(2)
    
    # 6. Cheapest & highest sites
    offers_df = sellers_clean[sellers_clean['seller_price'].notna()][['product_id', 'site_display', 'seller_price']]
    
    # O(N) single-pass lookup of cheapest and highest price indices (no sorting!)
    idx_min = offers_df.groupby('product_id')['seller_price'].idxmin()
    idx_max = offers_df.groupby('product_id')['seller_price'].idxmax()
    
    products['cheapest_site'] = products['product_id'].map(offers_df.loc[idx_min].set_index('product_id')['site_display']).fillna('')
    products['highest_site'] = products['product_id'].map(offers_df.loc[idx_max].set_index('product_id')['site_display']).fillna('')
    
    # 7. Position & Index
    products['my_position'] = "I am in the middle"
    products.loc[products['comp_count'] == 0, 'my_position'] = "I am unique"
    products.loc[(products['comp_count'] > 0) & (products['my_price'] <= 0), 'my_position'] = "N/A"
    
    valid_comp_mask = (products['comp_count'] > 0) & (products['my_price'] > 0)
    products.loc[valid_comp_mask & (products['my_price'] >= products['comp_max']), 'my_position'] = "I am highest"
    products.loc[valid_comp_mask & (products['my_price'] <= products['comp_min']), 'my_position'] = "I am cheapest"
    
    products['my_index'] = '-'
    valid_index_mask = (products['avg_price'] > 0) & (products['my_price'] > 0)
    products.loc[valid_index_mask, 'my_index'] = (
        (products.loc[valid_index_mask, 'my_price'] / products.loc[valid_index_mask, 'avg_price']) * 100
    ).round(2)
    
    # 8. Last update cycle
    results_mapped = results_df[['product_id', 'google_title', 'updated_at']].copy()
    results_mapped['updated_at'] = pd.to_datetime(results_mapped['updated_at']).fillna(now_dt)
    
    products = products.merge(
        results_mapped,
        on='product_id',
        how='left'
    )
    products['updated_at'] = products['updated_at'].fillna(now_dt)
    products['last_update_cycle'] = products['updated_at'].dt.strftime("%d %b %Y %H:%M (GMT ZZZZ)")
    
    # 9. Create final joined table
    sellers_sub = pd.DataFrame({
        'product_id': sellers_clean['product_id'],
        's_name': sellers_clean['seller_name'],
        's_price': sellers_clean['seller_price'],
        's_url': sellers_clean['seller_url'],
        's_stock': sellers_clean['stock_status'],
        'site_display': sellers_clean['site_display'],
        'is_me': sellers_clean['is_me']
    })
    
    report_df = products.merge(sellers_sub, on='product_id', how='left')
    
    # 10. Construct output DataFrames (efficiently using dict for single memory allocation)
    df1_data = {
        'Product Name': report_df['google_title'].fillna(report_df['name']).fillna(''),
        'Product Code': report_df['product_id'],
        'Barcode': report_df['gtin'].fillna(''),
        'Brand': report_df['brand'].fillna(''),
        'Category': report_df['category'].fillna(''),
        'Product Tags': '-',
        'Number of Matches': report_df['comp_count'],
        'My Index': report_df['my_index'],
        'My Position': report_df['my_position'],
        'Cheapest Site': report_df['cheapest_site'],
        'Highest Site': report_df['highest_site'],
        'Minimum Price': report_df['min_price'],
        'Maximum Price': report_df['max_price'],
        'Average Price': report_df['avg_price'],
        'My Price': report_df['my_price'],
        'My Product Cost': report_df['my_product_cost'],
        'Additional Cost': 0,
        'SmartPrice': '-',
        'Last Update Cycle': report_df['last_update_cycle'],
        'Site': report_df['site_display'],
        'Site Index': '-',
        'Price': report_df['s_price'].fillna(0.00),
        'Change direction': '-',
        'Stock': report_df['s_stock'],
        'URL': report_df['s_url'].fillna(report_df['osb_url']).fillna('')
    }
    
    cols1 = [
        'Product Name', 'Product Code', 'Barcode', 'Brand', 'Category',
        'Product Tags', 'Number of Matches', 'My Index', 'My Position',
        'Cheapest Site', 'Highest Site', 'Minimum Price', 'Maximum Price',
        'Average Price', 'My Price', 'My Product Cost', 'Additional Cost',
        'SmartPrice', 'Last Update Cycle', 'Site', 'Site Index', 'Price',
        'Change direction', 'Stock', 'URL'
    ]
    df1 = pd.DataFrame(df1_data, columns=cols1)
    
    # Efficiently reuse df1 to build df2 instead of allocating a new 10M row DataFrame from scratch
    df2 = df1.copy()
    df2 = df2.drop(columns=['Minimum Price', 'Maximum Price', 'Average Price', 'Price'])
    df2['Minimum Price (Total Price)'] = '-'
    df2['Maximum Price (Total Price)'] = '-'
    df2['Average Price (Total Price)'] = '-'
    df2['My Total Price'] = '-'
    df2['Total Price'] = df1['Price']
    
    cols2 = [
        'Product Name', 'Product Code', 'Barcode', 'Brand', 'Category',
        'Product Tags', 'Number of Matches', 'My Index', 'My Position',
        'Cheapest Site', 'Highest Site', 'Minimum Price (Total Price)',
        'Maximum Price (Total Price)', 'Average Price (Total Price)',
        'My Price', 'My Total Price', 'My Product Cost', 'Additional Cost',
        'SmartPrice', 'Last Update Cycle', 'Site', 'Site Index', 'Total Price',
        'Change direction', 'Stock', 'URL'
    ]
    df2 = df2[cols2]

    # Generate file names
    now = datetime.now()
    date_prefix = now.strftime("%Y.%m.%d-%H%M")
    ts_ms = int(time.time() * 1000)

    file1_name = f"{date_prefix}_1stopbedrooms_Prisync_Vertical_Report_price_change_stock_{ts_ms}.csv"
    file2_name = f"{date_prefix}_1stopbedrooms_Prisync_Vertical_Report_total_price_change_stock_{ts_ms + 1}.csv"

    # Zip the files directly using stream buffers (prevents writing giant intermediate CSVs to disk)
    zip_filename = "1stopbedrooms_export.zip"
    zip_path = os.path.join(os.getcwd(), zip_filename)
    
    print(f"Streaming CSVs directly into ZIP archive {zip_filename}...")
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            print(f"Compressing File 1 into ZIP...")
            with zipf.open(file1_name, 'w') as f1:
                df1.to_csv(f1, index=False, line_terminator='\n')
                
            print(f"Compressing File 2 into ZIP...")
            with zipf.open(file2_name, 'w') as f2:
                df2.to_csv(f2, index=False, line_terminator='\n')
                
        print(f"✓ ZIP archive successfully completed at: {zip_path}")
    except Exception as e:
        print(f"❌ Failed to build ZIP archive: {e}")
        sys.exit(1)

    # SFTP upload to Oracle server
    oracle_success = upload_to_oracle_sftp(zip_path, zip_filename)
    if oracle_success:
        print("✓ Export and SFTP upload completed successfully!")
        try:
            os.remove(zip_path)
        except Exception as e:
            print(f"Warning: Failed to remove local ZIP file: {e}")
        sys.exit(0)
    else:
        print("❌ SFTP upload failed.")
        sys.exit(1)

if __name__ == '__main__':
    main()
