import pandas as pd
import pymysql
import os
from dotenv import load_dotenv
import traceback
import sys

load_dotenv()

def execute_values_mysql(cursor, query_template, values):
    """Emulate psycopg2.extras.execute_values for MySQL."""
    if not values:
        return
    num_cols = len(values[0])
    row_placeholders = "(" + ", ".join(["%s"] * num_cols) + ")"
    all_placeholders = ", ".join([row_placeholders] * len(values))
    query = query_template.replace("%s", all_placeholders, 1)
    flat_args = []
    for row in values:
        flat_args.extend(row)
    cursor.execute(query, flat_args)

def import_csv(csv_path):
    try:
        mysql_host = os.environ.get("MYSQL_HOST")
        mysql_port = os.environ.get("MYSQL_PORT", "3306")
        try:
            mysql_port = int(mysql_port)
        except ValueError:
            mysql_port = 3306
        mysql_user = os.environ.get("MYSQL_USER")
        mysql_pass = os.environ.get("MYSQL_PASS")
        mysql_db = os.environ.get("MYSQL_DB")

        if not all([mysql_host, mysql_user, mysql_pass, mysql_db]):
            print("Error: Missing database credentials in .env")
            return

        print("Connecting to the MySQL database...")
        conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_pass,
            database=mysql_db,
            connect_timeout=10,
            read_timeout=60,
            write_timeout=60
        )
        cursor = conn.cursor()

        print(f"Reading CSV {csv_path}...")
        df = pd.read_csv(csv_path)
        
        initial_len = len(df)
        df = df.drop_duplicates(subset=['product_id'])
        print(f"Processing {len(df)} products (removed {initial_len - len(df)} duplicates).")

        insert_query = """
            INSERT INTO osb_products (
                product_id, web_id, name, sku, mpn, gtin, brand, product_type, keyword, url, osb_url, status, mfr_sales_30d
            )
            VALUES %s
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                mfr_sales_30d = VALUES(mfr_sales_30d),
                name = VALUES(name),
                keyword = VALUES(keyword)
        """

        batch_size = 5000
        total_rows = len(df)
        
        print("Starting batch import...")
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i : i + batch_size]
            records = batch.to_dict('records')
            values = []
            
            for row_dict in records:
                # Get sales count safely
                sales = row_dict.get('30daymfrsales', 0)
                if pd.isna(sales) or sales == '':
                    sales = 0
                else:
                    try:
                        sales = int(float(sales))
                    except (ValueError, TypeError):
                        sales = 0
                
                # Get status safely
                p_status = row_dict.get('status', 1)
                if pd.isna(p_status) or p_status == '':
                    p_status = 1
                else:
                    try:
                        p_status = int(float(p_status))
                    except (ValueError, TypeError):
                        p_status = 1

                # Clean any NaN values to empty string for other fields
                def clean(val):
                    return '' if pd.isna(val) else str(val)

                # Format GTIN safely as None or int
                gtin_val = row_dict.get('gtin')
                if pd.isna(gtin_val) or gtin_val == '':
                    gtin_clean = None
                else:
                    try:
                        gtin_clean = int(float(gtin_val))
                    except:
                        gtin_clean = None

                values.append((
                    int(row_dict.get('product_id')),
                    clean(row_dict.get('web_id')),
                    clean(row_dict.get('name')),
                    clean(row_dict.get('mpn_sku')),  # sku
                    clean(row_dict.get('mpn_sku')),  # mpn
                    gtin_clean,
                    clean(row_dict.get('brand')),
                    clean(row_dict.get('category')), # product_type
                    clean(row_dict.get('keyword')),
                    clean(row_dict.get('url')),
                    clean(row_dict.get('osb_url')),
                    p_status,
                    sales
                ))
            
            try:
                execute_values_mysql(cursor, insert_query, values)
                conn.commit()
                sys.stdout.write(f"\r✓ Imported {min(i + batch_size, total_rows)} / {total_rows} rows...")
                sys.stdout.flush()
            except Exception as batch_error:
                conn.rollback()
                print(f"\nError in batch starting at row {i}: {batch_error}")
                # Try smaller batch of 1000 for recovery
                print("Retrying with smaller batches...")
                for j in range(0, len(values), 1000):
                    sub_values = values[j : j + 1000]
                    try:
                        execute_values_mysql(cursor, insert_query, sub_values)
                        conn.commit()
                    except Exception as sub_err:
                        conn.rollback()
                        print(f"Failed to import sub-batch starting at index {i+j}: {sub_err}")

        print("\n✓ Import completed successfully.")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nError during import: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    import sys
    # Use first argument if provided, otherwise check for gshopping.csv, else fall back to sample CSV
    csv_file = "gshopping.csv"
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    elif not os.path.exists(csv_file):
        csv_file = "google_shopping_sample_moniter_pr_urls_pr_feed_analysis.csv"
        
    if os.path.exists(csv_file):
        print(f"Starting import for file: {csv_file}")
        import_csv(csv_file)
    else:
        print(f"❌ Error: CSV file '{csv_file}' not found.")
        sys.exit(1)
