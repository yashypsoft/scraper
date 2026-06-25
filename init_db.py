import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def init_db():
    try:
        mysql_host = os.getenv("MYSQL_HOST")
        mysql_port = os.getenv("MYSQL_PORT", "3306")
        try:
            mysql_port = int(mysql_port)
        except ValueError:
            mysql_port = 3306
        mysql_user = os.getenv("MYSQL_USER")
        mysql_pass = os.getenv("MYSQL_PASS")
        mysql_db = os.getenv("MYSQL_DB")

        print("Connecting to MySQL...")
        conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_pass,
            database=mysql_db
        )
        cursor = conn.cursor()
        
        schema_path = "data/sql/new_scraping_schema_mysql.sql"
        print(f"Executing schema from {schema_path}...")
        with open(schema_path, "r") as f:
            sql = f.read()
            
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                # Remove comments or empty lines
                lines = [line for line in stmt.split("\n") if not line.strip().startswith("--")]
                clean_stmt = "\n".join(lines).strip()
                if clean_stmt:
                    cursor.execute(clean_stmt)
            
        conn.commit()
        cursor.close()
        conn.close()
        print("✓ Database tables initialized successfully in MySQL.")
    except Exception as e:
        print(f"Error initializing database: {e}")

if __name__ == "__main__":
    init_db()
