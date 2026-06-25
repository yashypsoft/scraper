import pandas as pd
import pymysql
import os
import sys
import argparse
from dotenv import load_dotenv

# Load env variables from root folder
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

def clean_str(val):
    if pd.isna(val) or val is None:
        return ""
    val_str = str(val).strip()
    if val_str.lower() in ("nan", "null", "none"):
        return ""
    return val_str

def clean_int(val, default=0):
    if pd.isna(val) or val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

def clean_float(val):
    if pd.isna(val) or val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def clean_gtin(val):
    if pd.isna(val) or val is None or val == '':
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None

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

def main():
    parser = argparse.ArgumentParser(description="Import product details from system.csv into osb_products table.")
    parser.add_argument("--csv", type=str, default="gshopping/system.csv", help="Path to system.csv")
    parser.add_argument("--limit", type=int, default=None, help="Process only the first N rows for testing")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for database upserts")
    
    args = parser.parse_args()
    
    csv_path = args.csv
    if not os.path.isabs(csv_path):
        csv_path = os.path.join(os.path.dirname(__file__), "..", csv_path)
        
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file '{csv_path}' not found.")
        sys.exit(1)

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
        print("❌ Error: Missing database credentials in .env")
        sys.exit(1)

    print("Connecting to MySQL...")
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
    print("✓ DB Connected successfully!")

    insert_query = """
        INSERT INTO osb_products (
            product_id, name, sku, web_id, gtin, status, mpn, brand, collection, product_type,
            grouping_attr_1, grouping_attr_2, grouping_attr_1_value, grouping_attr_2_value,
            part_number, osb_url, mfr_sales_30d, margin, price, map_price, color,
            bed_size_measure, size, fireplace_option, layout_icon, rug_size, mattress_size,
            power_option, dimension_text, comfort_level, mattress_thickness
        ) VALUES %s
        ON DUPLICATE KEY UPDATE
            name = COALESCE(NULLIF(VALUES(name), ''), name),
            sku = COALESCE(NULLIF(VALUES(sku), ''), sku),
            web_id = COALESCE(NULLIF(VALUES(web_id), ''), web_id),
            gtin = COALESCE(NULLIF(VALUES(gtin), 0), gtin),
            status = COALESCE(VALUES(status), status),
            mpn = COALESCE(NULLIF(VALUES(mpn), ''), mpn),
            brand = COALESCE(NULLIF(VALUES(brand), ''), brand),
            collection = COALESCE(NULLIF(VALUES(collection), ''), collection),
            product_type = COALESCE(NULLIF(VALUES(product_type), ''), product_type),
            grouping_attr_1 = COALESCE(NULLIF(VALUES(grouping_attr_1), ''), grouping_attr_1),
            grouping_attr_2 = COALESCE(NULLIF(VALUES(grouping_attr_2), ''), grouping_attr_2),
            grouping_attr_1_value = COALESCE(NULLIF(VALUES(grouping_attr_1_value), ''), grouping_attr_1_value),
            grouping_attr_2_value = COALESCE(NULLIF(VALUES(grouping_attr_2_value), ''), grouping_attr_2_value),
            part_number = COALESCE(NULLIF(VALUES(part_number), ''), part_number),
            osb_url = COALESCE(NULLIF(VALUES(osb_url), ''), osb_url),
            mfr_sales_30d = COALESCE(NULLIF(VALUES(mfr_sales_30d), 0), mfr_sales_30d),
            margin = COALESCE(VALUES(margin), margin),
            price = COALESCE(VALUES(price), price),
            map_price = COALESCE(VALUES(map_price), map_price),
            color = COALESCE(NULLIF(VALUES(color), ''), color),
            bed_size_measure = COALESCE(NULLIF(VALUES(bed_size_measure), ''), bed_size_measure),
            size = COALESCE(NULLIF(VALUES(size), ''), size),
            fireplace_option = COALESCE(NULLIF(VALUES(fireplace_option), ''), fireplace_option),
            layout_icon = COALESCE(NULLIF(VALUES(layout_icon), ''), layout_icon),
            rug_size = COALESCE(NULLIF(VALUES(rug_size), ''), rug_size),
            mattress_size = COALESCE(NULLIF(VALUES(mattress_size), ''), mattress_size),
            power_option = COALESCE(NULLIF(VALUES(power_option), ''), power_option),
            dimension_text = COALESCE(NULLIF(VALUES(dimension_text), ''), dimension_text),
            comfort_level = COALESCE(NULLIF(VALUES(comfort_level), ''), comfort_level),
            mattress_thickness = COALESCE(NULLIF(VALUES(mattress_thickness), ''), mattress_thickness),
            updated_at = CURRENT_TIMESTAMP
    """

    print(f"Reading {csv_path} in chunks of {args.batch_size}...")
    chunk_idx = 0
    total_processed = 0
    
    chunks = pd.read_csv(csv_path, chunksize=args.batch_size)
    
    for df_chunk in chunks:
        df_chunk = df_chunk.drop_duplicates(subset=['product_id'])
        
        values = []
        for _, row in df_chunk.iterrows():
            prod_id = clean_str(row.get('product_id'))
            if not prod_id:
                continue
                
            values.append((
                int(float(prod_id)),
                clean_str(row.get('product_name')),
                clean_str(row.get('sku')),
                clean_str(row.get('web_id')),
                clean_gtin(row.get('gtin')),
                clean_int(row.get('status'), default=1),
                clean_str(row.get('mpn')),
                clean_str(row.get('brand_label')),
                clean_str(row.get('collection')),
                clean_str(row.get('cat')),
                clean_str(row.get('Group Attr 1')),
                clean_str(row.get('Group Attr 2')),
                clean_str(row.get('Group Attr 1 Value')),
                clean_str(row.get('Group Attr 2 Value')),
                clean_str(row.get('part_number')),
                clean_str(row.get('osb_url')),
                clean_int(row.get('30 days MFR Sales'), default=0),
                clean_float(row.get('margin')),
                clean_float(row.get('our_price')),
                clean_float(row.get('map_price')),
                clean_str(row.get('color')),
                clean_str(row.get('bed_size_measure')),
                clean_str(row.get('size')),
                clean_str(row.get('fireplace_option')),
                clean_str(row.get('layout_icon')),
                clean_str(row.get('rug_size')),
                clean_str(row.get('mattress_size')),
                clean_str(row.get('power_option')),
                clean_str(row.get('dimension_text')),
                clean_str(row.get('comfort_level')),
                clean_str(row.get('mattress_thickness'))
            ))
            
        if values:
            try:
                execute_values_mysql(cursor, insert_query, values)
                conn.commit()
                total_processed += len(values)
                print(f"✓ Processed {total_processed} rows...")
            except Exception as batch_error:
                conn.rollback()
                print(f"❌ Error in chunk {chunk_idx}: {batch_error}")
                # Fallback: try row-by-row for this chunk
                print("Retrying this chunk row-by-row...")
                row_err_count = 0
                for v in values:
                    try:
                        execute_values_mysql(cursor, insert_query, [v])
                        conn.commit()
                        total_processed += 1
                    except Exception as row_error:
                        conn.rollback()
                        row_err_count += 1
                        if row_err_count <= 5:
                            print(f"  Failed row product_id {v[0]}: {row_error}")
                if row_err_count > 5:
                    print(f"  ... and {row_err_count - 5} other rows failed in this chunk.")
        
        chunk_idx += 1
        if args.limit and total_processed >= args.limit:
            print(f"Stopping import due to limit threshold ({args.limit} rows).")
            break

    cursor.close()
    conn.close()
    print(f"🎉 Import complete! Successfully upserted/updated {total_processed} rows in total.")

if __name__ == "__main__":
    main()
