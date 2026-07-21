import os
import pymysql
import requests
import json
import time

def get_db_credentials():
    creds = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    parts = line.strip().split("=", 1)
                    if len(parts) == 2:
                        creds[parts[0].strip()] = parts[1].strip()
    return creds

def get_boundaries():
    creds = get_db_credentials()
    host = creds.get("MYSQL_HOST")
    port = int(creds.get("MYSQL_PORT", 3306))
    user = creds.get("MYSQL_USER")
    password = creds.get("MYSQL_PASS")
    db = creds.get("MYSQL_DB")

    print(f"Connecting to database {db} at {host}:{port}...")
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db
    )
    cursor = conn.cursor()
    
    query = """
    WITH partitioned_products AS (
        SELECT 
            product_id,
            mfr_sales_30d,
            NTILE(4) OVER (ORDER BY COALESCE(mfr_sales_30d, 0) DESC, product_id ASC) as bucket
        FROM osb_products
        WHERE status = 1
    )
    SELECT 
        bucket,
        MAX(COALESCE(mfr_sales_30d, 0)) AS start_sales_bound,
        MIN(product_id) AS start_id_bound,
        MIN(COALESCE(mfr_sales_30d, 0)) AS end_sales_bound,
        MAX(product_id) AS end_id_bound,
        COUNT(*) AS product_count
    FROM partitioned_products
    GROUP BY bucket
    ORDER BY bucket;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    buckets = {}
    for r in rows:
        bucket = r[0]
        buckets[str(bucket)] = {
            "start_sales_bound": str(r[1]),
            "start_id_bound": str(r[2]),
            "end_sales_bound": str(r[3]),
            "end_id_bound": str(r[4]),
            "product_count": r[5]
        }
    return buckets

def cancel_active_runs(account_config):
    owner = account_config["owner"]
    repo = account_config["repo"]
    workflow = account_config["workflow"]
    token = account_config["token"]
    
    # We query status=in_progress runs
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow}/runs?status=in_progress"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to fetch runs for {owner}/{repo}: {response.status_code} - {response.text}")
            return
            
        data = response.json()
        runs = data.get("workflow_runs", [])
        if not runs:
            print(f"No active runs found for {account_config['name']}.")
            return
            
        for run in runs:
            run_id = run["id"]
            cancel_url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs/{run_id}/cancel"
            print(f"Cancelling active run {run_id} for {account_config['name']}...")
            cancel_resp = requests.post(cancel_url, headers=headers)
            if cancel_resp.status_code == 202:
                print(f"\u2713 Successfully cancelled run {run_id}!")
            else:
                print(f"\u2717 Failed to cancel run {run_id}: {cancel_resp.status_code} - {cancel_resp.text}")
    except Exception as e:
        print(f"Error cancelling runs for {account_config['name']}: {e}")

def trigger_workflow(account_config, start_sales, start_id, end_sales, end_id):
    owner = account_config["owner"]
    repo = account_config["repo"]
    workflow = account_config["workflow"]
    token = account_config["token"]
    
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    payload = {
        "ref": account_config["ref"],
        "inputs": {
            "total_chunks": account_config["total_chunks"],
            "products_per_hour": account_config["products_per_hour"],
            "max_runtime_hours": account_config["max_runtime_hours"],
            "claim_limit": account_config["claim_limit"],
            "max_depth": account_config["max_depth"],
            "reset_errors": account_config["reset_errors"],
            "start_sales": start_sales,
            "start_id": start_id,
            "end_sales": end_sales,
            "end_id": end_id
        }
    }
    
    print(f"Triggering {account_config['name']} ({owner}/{repo})...")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 204:
        print(f"\u2713 Successfully triggered run for {account_config['name']}!")
        return True
    else:
        print(f"\u2717 Failed to trigger {account_config['name']}: {response.status_code} - {response.text}")
        return False

def main():
    config_path = "config/accounts_config.json"
    if not os.path.exists(config_path):
        print(f"Error: configuration file {config_path} not found.")
        return
        
    with open(config_path, "r") as f:
        accounts = json.load(f)
        
    # First, cancel active runs for all accounts
    print("Stopping active workflow runs across all accounts...")
    for acc_id in sorted(accounts.keys()):
        cancel_active_runs(accounts[acc_id])
        
    # Wait a few seconds to let cancellation propagate
    print("Waiting 5 seconds for cancellations to propagate...")
    time.sleep(5)
        
    try:
        buckets = get_boundaries()
    except Exception as e:
        print(f"Error reading database boundaries: {e}")
        return
        
    print("\nCalculated Boundaries:")
    for b_id, b_data in buckets.items():
        print(f"Bucket {b_id}: Sales {b_data['start_sales_bound']} (ID {b_data['start_id_bound']}) to Sales {b_data['end_sales_bound']} (ID {b_data['end_id_bound']}) - count: {b_data['product_count']}")

    print("\nAssigning boundaries and triggering workflows...")
    
    # Run 1 (Bucket 1)
    trigger_workflow(
        accounts["1"],
        start_sales="",
        start_id="",
        end_sales=buckets["1"]["end_sales_bound"],
        end_id=buckets["1"]["end_id_bound"]
    )
    
    # Run 2 (Bucket 2)
    trigger_workflow(
        accounts["2"],
        start_sales=buckets["2"]["start_sales_bound"],
        start_id=buckets["2"]["start_id_bound"],
        end_sales=buckets["2"]["end_sales_bound"],
        end_id=buckets["2"]["end_id_bound"]
    )
    
    # Run 3 (Bucket 3)
    trigger_workflow(
        accounts["3"],
        start_sales=buckets["3"]["start_sales_bound"],
        start_id=buckets["3"]["start_id_bound"],
        end_sales=buckets["3"]["end_sales_bound"],
        end_id=buckets["3"]["end_id_bound"]
    )
    
    # Run 4 (Bucket 4)
    trigger_workflow(
        accounts["4"],
        start_sales=buckets["4"]["start_sales_bound"],
        start_id=buckets["4"]["start_id_bound"],
        end_sales="",
        end_id=""
    )

if __name__ == "__main__":
    main()
