import os
import sys
import time
import json
import datetime
import psycopg2
import pymysql
from dotenv import load_dotenv

# Load environment variables
load_dotenv("/Users/yashprajapati/scrap/.env")

# PostgreSQL Source Configuration
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
try:
    PG_PORT = int(PG_PORT)
except ValueError:
    PG_PORT = 5432
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_DB = os.getenv("PG_DB", "scrapper_db")

# MySQL Destination Configuration
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
try:
    MYSQL_PORT = int(MYSQL_PORT)
except ValueError:
    MYSQL_PORT = 3306
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")
MYSQL_DB = os.getenv("MYSQL_DB")

# Primary key configurations for resumable logic
TABLE_PKS = {
    'competitors': 'competitor_id',
    'osb_products': 'product_id',
    'google_shopping_results': 'product_id',
    'google_shopping_sellers': 'seller_listing_id',
    'seller_scrape_jobs': 'scraping_id',
    'seller_product_details': 'seller_product_detail_id'
}

# Table dependency order
MIGRATION_ORDER = [
    'competitors',
    'osb_products',
    'google_shopping_results',
    'google_shopping_sellers',
    'seller_scrape_jobs',
    'seller_product_details'
]

def connect_pg():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        dbname=PG_DB,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

def connect_mysql():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        charset='utf8mb4',
        connect_timeout=10,
        read_timeout=60,
        write_timeout=60
    )

class MigrationState:
    def __init__(self):
        self.pg_conn = None
        self.mysql_conn = None
        self.mysql_cursor = None

    def connect(self):
        print(f"Connecting to PostgreSQL source ({PG_HOST}:{PG_PORT}/{PG_DB})...")
        self.pg_conn = connect_pg()
        self.pg_conn.autocommit = True
        
        # Set session statement timeout for PG
        pg_cursor = self.pg_conn.cursor()
        pg_cursor.execute("SET statement_timeout = 60000;")
        pg_cursor.close()
        print("✓ Connected to PostgreSQL and set statement timeout.")

        print(f"Connecting to MySQL destination ({MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB})...")
        self.mysql_conn = connect_mysql()
        self.mysql_cursor = self.mysql_conn.cursor()
        print("✓ Connected to MySQL.")

        self.optimize_mysql()

    def optimize_mysql(self):
        try:
            self.mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            self.mysql_cursor.execute("SET UNIQUE_CHECKS = 0;")
            print("✓ Optimizations applied: unique/foreign key constraints disabled on session.")
        except Exception as e:
            print(f"⚠️ Warning: Could not disable constraints: {e}")

    def reconnect_pg(self):
        try:
            self.pg_conn.close()
        except:
            pass
        for attempt in range(1, 6):
            try:
                print(f"  [pg-reconnect] Attempt {attempt}/5...")
                self.pg_conn = connect_pg()
                self.pg_conn.autocommit = True
                
                # Set session statement timeout for PG
                pg_cursor = self.pg_conn.cursor()
                pg_cursor.execute("SET statement_timeout = 60000;")
                pg_cursor.close()
                
                print("  ✓ [pg-reconnect] Success.")
                return
            except Exception as e:
                print(f"  ⚠️ [pg-reconnect] Failed: {e}")
                time.sleep(5)
        raise Exception("Fatal: Failed to reconnect to PostgreSQL source.")

    def reconnect_mysql(self):
        try:
            self.mysql_conn.close()
        except:
            pass
        for attempt in range(1, 6):
            try:
                print(f"  [mysql-reconnect] Attempt {attempt}/5...")
                self.mysql_conn = connect_mysql()
                self.mysql_cursor = self.mysql_conn.cursor()
                self.optimize_mysql()
                print("  ✓ [mysql-reconnect] Success.")
                return
            except Exception as e:
                print(f"  ⚠️ [mysql-reconnect] Failed: {e}")
                time.sleep(5)
        raise Exception("Fatal: Failed to reconnect to MySQL destination.")

def process_val(val):
    """
    Format values from PostgreSQL so they are fully compatible with MySQL.
    - Convert list/dict to serialized JSON strings.
    - Strip timezone information from datetimes (convert to naive UTC).
    """
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    if isinstance(val, datetime.datetime):
        if val.tzinfo is not None:
            return val.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return val
    return val

def migrate_table(state, table_name, pk_col, batch_size=10000):
    pg_cursor = state.pg_conn.cursor()
    
    # 1. Retrieve the ordered column names from PostgreSQL
    pg_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position
    """, (table_name,))
    all_columns = [row[0] for row in pg_cursor.fetchall()]
    
    if not all_columns:
        print(f"❌ Error: Table '{table_name}' not found in PostgreSQL source schema.")
        return
        
    # Exclude generated/virtual columns that MySQL handles automatically
    cols_to_insert = [col for col in all_columns if col != 'seller_url_hash']
    
    # 2. Determine current state for resumability
    state.mysql_cursor.execute(f"SELECT MAX({pk_col}) FROM {table_name}")
    max_id = state.mysql_cursor.fetchone()[0]
    if max_id is None:
        max_id = 0
        
    # 3. Count total records remaining to migrate
    pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {pk_col} > %s", (max_id,))
    total_to_migrate = pg_cursor.fetchone()[0]
    
    print(f"=== Migrating table '{table_name}' ===")
    print(f"  - Primary Key: {pk_col}")
    print(f"  - Resuming from {pk_col} > {max_id}")
    print(f"  - Rows remaining: {total_to_migrate}")
    
    if total_to_migrate == 0:
        print(f"  ✓ Table '{table_name}' is already up-to-date.")
        return
        
    # Queries Setup
    cols_str = ", ".join(f"`{c}`" for c in cols_to_insert) # Use backticks for MySQL safety
    pg_cols_str = ", ".join(f'"{c}"' for c in cols_to_insert) # Use quotes for PG safety
    pg_query = f"SELECT {pg_cols_str} FROM {table_name} WHERE {pk_col} > %s ORDER BY {pk_col} ASC LIMIT %s"
    
    placeholders = ", ".join(["%s"] * len(cols_to_insert))
    insert_template = f"INSERT IGNORE INTO {table_name} ({cols_str}) VALUES "
    
    migrated_count = 0
    start_time = time.time()
    last_log_time = time.time()
    current_max_id = max_id
    
    # Identify the index of the PK column in our insertion list
    pk_idx = cols_to_insert.index(pk_col)
    
    while True:
        # Fetch batch from PostgreSQL with retries
        rows = None
        for attempt in range(1, 4):
            try:
                pg_cursor.execute(pg_query, (current_max_id, batch_size))
                rows = pg_cursor.fetchall()
                break
            except Exception as e:
                print(f"  ⚠️ Fetch attempt {attempt}/3 failed for {table_name} at ID {current_max_id}: {e}")
                if attempt == 3:
                    raise e
                time.sleep(5)
                state.reconnect_pg()
                pg_cursor = state.pg_conn.cursor()
        
        if not rows:
            break
            
        # Format and convert the values
        processed_rows = []
        for row in rows:
            processed_row = tuple(process_val(val) for val in row)
            processed_rows.append(processed_row)
            
        # Build the bulk multi-value insert SQL statement
        row_placeholders = f"({placeholders})"
        all_placeholders = ", ".join([row_placeholders] * len(processed_rows))
        mysql_query = insert_template + all_placeholders
        
        # Flatten arguments list
        flat_args = []
        for pr in processed_rows:
            flat_args.extend(pr)
            
        # Execute insert with retries
        for attempt in range(1, 4):
            try:
                state.mysql_cursor.execute(mysql_query, flat_args)
                state.mysql_conn.commit()
                break
            except Exception as e:
                try:
                    state.mysql_conn.rollback()
                except Exception as re:
                    pass  # Connection might be dead, which is expected if the execution failed due to a lost connection
                print(f"  ⚠️ Insert attempt {attempt}/3 failed for {table_name} at ID {current_max_id}: {e}")
                if attempt == 3:
                    raise e
                time.sleep(5)
                state.reconnect_mysql()
            
        # Update current PK tracker to the last row's PK
        current_max_id = rows[-1][pk_idx]
        migrated_count += len(rows)
        
        # Logging progress at intervals
        now = time.time()
        if now - last_log_time >= 5 or migrated_count == total_to_migrate:
            elapsed = now - start_time
            rate = migrated_count / elapsed if elapsed > 0 else 0
            pct = (migrated_count / total_to_migrate) * 100
            print(f"  -> Migrated {migrated_count}/{total_to_migrate} ({pct:.2f}%) | Speed: {rate:.1f} rows/s")
            last_log_time = now
            
    total_elapsed = time.time() - start_time
    print(f"✓ Completed '{table_name}': {migrated_count} rows migrated in {total_elapsed:.2f}s.\n")

def migrate_sellers_table(state, table_name, pk_col, chunk_size=2000):
    pg_cursor = state.pg_conn.cursor()
    
    # 1. Retrieve the ordered column names from PostgreSQL
    pg_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position
    """, (table_name,))
    all_columns = [row[0] for row in pg_cursor.fetchall()]
    cols_to_insert = [col for col in all_columns if col != 'seller_url_hash']
    
    # 2. Determine current max product_id in MySQL to resume
    state.mysql_cursor.execute("SELECT MAX(product_id) FROM google_shopping_sellers")
    max_prod_id = state.mysql_cursor.fetchone()[0]
    if max_prod_id is None:
        max_prod_id = 0
        
    # 3. Retrieve min and max product_id from PostgreSQL
    pg_cursor.execute("SELECT MIN(product_id), MAX(product_id) FROM google_shopping_sellers")
    pg_min_prod_id, pg_max_prod_id = pg_cursor.fetchone()
    
    if pg_min_prod_id is None:
        print(f"✓ Table '{table_name}' is empty on PostgreSQL source.")
        return
        
    start_prod_id = max(max_prod_id, pg_min_prod_id)
    print(f"=== Migrating table '{table_name}' via product_id chunking ===")
    print(f"  - Product ID range in PG: {pg_min_prod_id} to {pg_max_prod_id}")
    print(f"  - Resuming from product_id >= {start_prod_id}")
    
    # Queries Setup
    cols_str = ", ".join(f"`{c}`" for c in cols_to_insert) # Use backticks for MySQL safety
    pg_cols_str = ", ".join(f'"{c}"' for c in cols_to_insert) # Use quotes for PG safety
    pg_query = f"SELECT {pg_cols_str} FROM {table_name} WHERE product_id >= %s AND product_id < %s"
    
    placeholders = ", ".join(["%s"] * len(cols_to_insert))
    insert_template = f"INSERT IGNORE INTO {table_name} ({cols_str}) VALUES "
    
    current_prod_id = start_prod_id
    migrated_count = 0
    start_time = time.time()
    last_log_time = time.time()
    
    while current_prod_id <= pg_max_prod_id:
        next_prod_id = current_prod_id + chunk_size
        
        # Fetch batch from PostgreSQL with retries
        rows = None
        for attempt in range(1, 4):
            try:
                pg_cursor.execute(pg_query, (current_prod_id, next_prod_id))
                rows = pg_cursor.fetchall()
                break
            except Exception as e:
                print(f"  ⚠️ Fetch attempt {attempt}/3 failed for {table_name} at product_id range [{current_prod_id}, {next_prod_id}]: {e}")
                if attempt == 3:
                    raise e
                time.sleep(5)
                state.reconnect_pg()
                pg_cursor = state.pg_conn.cursor()
                
        if not rows:
            current_prod_id = next_prod_id
            continue
            
        # Format and convert the values
        processed_rows = []
        for row in rows:
            processed_row = tuple(process_val(val) for val in row)
            processed_rows.append(processed_row)
            
        # Build the bulk multi-value insert SQL statement
        row_placeholders = f"({placeholders})"
        all_placeholders = ", ".join([row_placeholders] * len(processed_rows))
        mysql_query = insert_template + all_placeholders
        
        # Flatten arguments list
        flat_args = []
        for pr in processed_rows:
            flat_args.extend(pr)
            
        # Execute insert with retries
        for attempt in range(1, 4):
            try:
                state.mysql_cursor.execute(mysql_query, flat_args)
                state.mysql_conn.commit()
                break
            except Exception as e:
                try:
                    state.mysql_conn.rollback()
                except Exception as re:
                    pass
                print(f"  ⚠️ Insert attempt {attempt}/3 failed for {table_name} at product_id range [{current_prod_id}, {next_prod_id}]: {e}")
                if attempt == 3:
                    raise e
                time.sleep(5)
                state.reconnect_mysql()
                
        migrated_count += len(rows)
        current_prod_id = next_prod_id
        time.sleep(0.05)  # Let the database connections breathe
        
        # Logging progress
        now = time.time()
        if now - last_log_time >= 5 or current_prod_id > pg_max_prod_id:
            elapsed = now - start_time
            rate = migrated_count / elapsed if elapsed > 0 else 0
            total_range = pg_max_prod_id - start_prod_id
            current_range = current_prod_id - start_prod_id
            pct = (current_range / total_range) * 100 if total_range > 0 else 100
            print(f"  -> Product ID: {current_prod_id}/{pg_max_prod_id} ({pct:.2f}%) | Migrated: {migrated_count} rows | Speed: {rate:.1f} rows/s")
            last_log_time = now
            
    total_elapsed = time.time() - start_time
    print(f"✓ Completed '{table_name}': {migrated_count} rows migrated in {total_elapsed:.2f}s.\n")

def main():
    print("Starting database migration from PostgreSQL to MySQL...")
    
    state = MigrationState()
    try:
        state.connect()
    except Exception as e:
        print(f"❌ Initial connections failed: {e}")
        sys.exit(1)
        
    try:
        overall_start = time.time()
        for table_name in MIGRATION_ORDER:
            pk_col = TABLE_PKS[table_name]
            if table_name == 'google_shopping_sellers':
                # Bypassing LIMIT pagination by chunking product_id using the product_id index
                migrate_sellers_table(state, table_name, pk_col, chunk_size=500)
            else:
                # Adjust batch sizes: larger for simple/numeric tables, smaller for text-heavy tables
                if table_name in ['google_shopping_results', 'seller_product_details']:
                    batch_size = 400    # Very small batches due to massive raw HTML / JSON columns to avoid network drops
                else:
                    batch_size = 5000   # Default
                    
                migrate_table(state, table_name, pk_col, batch_size=batch_size)
            
        print(f"All tables successfully migrated! Total execution time: {time.time() - overall_start:.2f}s")
        
    except Exception as e:
        print(f"❌ Migration interrupted due to fatal error: {e}")
    finally:
        # Re-enable constraint checks
        try:
            print("Restoring MySQL session constraint checks...")
            state.mysql_cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            state.mysql_cursor.execute("SET UNIQUE_CHECKS = 1;")
            print("✓ Constraints restored.")
        except Exception as ce:
            print(f"⚠️ Warning: Could not restore database checks: {ce}")
            
        try:
            state.pg_conn.close()
            state.mysql_conn.close()
        except:
            pass
        print("Database connections closed.")

if __name__ == "__main__":
    main()
