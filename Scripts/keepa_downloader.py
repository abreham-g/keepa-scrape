import json
import time
import pathlib
import urllib.parse as up
import os
import sys
from typing import Any, Dict, List, Optional, Tuple
from collections import deque
from datetime import datetime

import requests
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ===== CONFIG FROM .ENV =====
# Read all configuration from environment variables with defaults
SHARD_FIELD = os.environ.get("SHARD_FIELD", "avg90_SALES")
SHARD_SIZE = int(os.environ.get("SHARD_SIZE", "10000"))
PAGING_ORDER = os.environ.get("PAGING_ORDER", "oldest")
SNAPSHOT_FREEZE = os.environ.get("SNAPSHOT_FREEZE", "True").lower() == "true"
REQS_PER_MINUTE = int(os.environ.get("REQS_PER_MINUTE", "60"))
REQS_PER_HOUR = int(os.environ.get("REQS_PER_HOUR", "3600"))
TOKENS_RESERVE = int(os.environ.get("TOKENS_RESERVE", "50"))
TOKENS_POLL_SEC = int(os.environ.get("TOKENS_POLL_SEC", "10"))

# Database configuration from .env
DB_SCHEMA = os.environ.get("DB_SCHEMA", "keepa_scrape")
DB_TABLE = os.environ.get("DB_TABLE", "Downloaded_asin")


# ===== CATEGORY CONFIGURATION =====
def get_categories_from_env():
    """Extract categories from environment variables."""
    categories = []
    
    # Define category mappings from env variable names to display names
    category_mappings = {
        'ELECTRONICS': 'ELECTRONICS',
        'CELL_PHONES_AND_ACCESSORIES': 'CELL_PHONES_AND_ACCESSORIES',
        'MUSICAL_INSTRUMENTS': 'MUSICAL_INSTRUMENTS',
        'MOVIES_AND_TV': 'MOVIES_AND_TV',
        'VIDEO_GAMES': 'VIDEO_GAMES',
        'HEALTH_AND_HOUSEHOLD': 'HEALTH_AND_HOUSEHOLD',
        'HOME_AND_KITCHEN': 'HOME_AND_KITCHEN'
    }
    
    for env_name, display_name in category_mappings.items():
        url = os.environ.get(env_name)
        if url:
            # Extract API key from URL for use in KeepaClient
            if 'key=' in url:
                parsed = up.urlparse(url)
                query_params = up.parse_qs(parsed.query)
                api_key = query_params.get('key', [''])[0]
            else:
                api_key = None
                
            categories.append({
                'name': display_name,
                'env_name': env_name,
                'url': url,
                'api_key': api_key
            })
            print(f"✓ Found category: {display_name}")
    
    if not categories:
        print("⚠️  No categories found in .env file!")
        print("Please add categories like:")
        print('ELECTRONICS="https://api.keepa.com/query?key=YOUR_KEY&domain=1&selection=..."')
    
    return categories


# ===== DATABASE UTILS =====
def get_db_connection():
    """Get database connection from environment variable."""
    conn_string = os.environ.get("DATABASE_URL")
    
    if not conn_string:
        print("Error: DATABASE_URL environment variable not set")
        print("Please set it in your .env file: DATABASE_URL=postgresql://user:pass@host:port/dbname")
        sys.exit(1)
    
    try:
        conn = psycopg2.connect(conn_string)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        sys.exit(1)


def create_schema_and_table():
    """Create schema and table with category column, adding column if it doesn't exist."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Create schema if not exists
        create_schema_sql = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(DB_SCHEMA)
        )
        cursor.execute(create_schema_sql)
        print(f"✓ Schema '{DB_SCHEMA}' is ready")
        
        # 2. Check if table exists and has category column
        cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = %s
        );
        """, (DB_SCHEMA, DB_TABLE))
        
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print(f"✓ Table '{DB_SCHEMA}.{DB_TABLE}' exists")
            
            # Check if category column exists
            cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = %s 
            AND column_name = 'category';
            """, (DB_SCHEMA, DB_TABLE))
            
            category_column_exists = cursor.fetchone() is not None
            
            if not category_column_exists:
                print(f"⚠️  Table exists but missing 'category' column. Adding it now...")
                try:
                    # First add the column (nullable)
                    alter_sql = sql.SQL("""
                    ALTER TABLE {}.{} 
                    ADD COLUMN category VARCHAR(100);
                    """).format(
                        sql.Identifier(DB_SCHEMA),
                        sql.Identifier(DB_TABLE)
                    )
                    cursor.execute(alter_sql)
                    
                    # Set default value for existing rows
                    update_sql = sql.SQL("""
                    UPDATE {}.{} 
                    SET category = 'UNCATEGORIZED' 
                    WHERE category IS NULL;
                    """).format(
                        sql.Identifier(DB_SCHEMA),
                        sql.Identifier(DB_TABLE)
                    )
                    cursor.execute(update_sql)
                    
                    # Add NOT NULL constraint
                    alter_not_null_sql = sql.SQL("""
                    ALTER TABLE {}.{} 
                    ALTER COLUMN category SET NOT NULL;
                    """).format(
                        sql.Identifier(DB_SCHEMA),
                        sql.Identifier(DB_TABLE)
                    )
                    cursor.execute(alter_not_null_sql)
                    
                    print(f"✓ Successfully added 'category' column to existing table")
                except Exception as e:
                    print(f"Error adding category column: {e}")
                    conn.rollback()
                    raise
            else:
                print(f"✓ Table already has 'category' column")
        else:
            # Create new table with category column
            print(f"Creating new table '{DB_SCHEMA}.{DB_TABLE}'...")
            create_table_sql = sql.SQL("""
            CREATE TABLE {}.{} (
                asin VARCHAR(20) PRIMARY KEY,
                amazon_url TEXT NOT NULL,
                category VARCHAR(100) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """).format(
                sql.Identifier(DB_SCHEMA),
                sql.Identifier(DB_TABLE)
            )
            cursor.execute(create_table_sql)
            print(f"✓ Created new table with 'category' column")
        
        # 3. Create or update indexes for better performance
        indexes_to_create = [
            (f"idx_{DB_TABLE}_updated", "updated_at"),
            (f"idx_{DB_TABLE}_category", "category"),
        ]
        
        for idx_name, column in indexes_to_create:
            try:
                # Check if index exists
                cursor.execute("""
                SELECT indexname FROM pg_indexes 
                WHERE schemaname = %s 
                AND tablename = %s 
                AND indexname = %s;
                """, (DB_SCHEMA, DB_TABLE, idx_name))
                
                if not cursor.fetchone():
                    create_index_sql = sql.SQL("""
                    CREATE INDEX {} ON {}.{}({});
                    """).format(
                        sql.Identifier(idx_name),
                        sql.Identifier(DB_SCHEMA),
                        sql.Identifier(DB_TABLE),
                        sql.Identifier(column)
                    )
                    cursor.execute(create_index_sql)
                    print(f"✓ Created index: {idx_name}")
                else:
                    print(f"✓ Index already exists: {idx_name}")
            except Exception as e:
                print(f"Note: Could not create index {idx_name}: {e}")
        
        # Create composite index for asin and category
        try:
            composite_idx_name = f"idx_{DB_TABLE}_asin_category"
            cursor.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE schemaname = %s 
            AND tablename = %s 
            AND indexname = %s;
            """, (DB_SCHEMA, DB_TABLE, composite_idx_name))
            
            if not cursor.fetchone():
                create_composite_sql = sql.SQL("""
                CREATE INDEX {} ON {}.{}(asin, category);
                """).format(
                    sql.Identifier(composite_idx_name),
                    sql.Identifier(DB_SCHEMA),
                    sql.Identifier(DB_TABLE)
                )
                cursor.execute(create_composite_sql)
                print(f"✓ Created composite index: {composite_idx_name}")
            else:
                print(f"✓ Composite index already exists: {composite_idx_name}")
        except Exception as e:
            print(f"Note: Could not create composite index: {e}")
        
        conn.commit()
        
        # 4. Show table info
        cursor.execute(sql.SQL("""
            SELECT COUNT(*) as total_count 
            FROM {}.{}
        """).format(
            sql.Identifier(DB_SCHEMA),
            sql.Identifier(DB_TABLE)
        ))
        
        count_result = cursor.fetchone()
        print(f"✓ Total records in table: {count_result[0]}")
        
        # 5. Show category distribution
        try:
            cursor.execute(sql.SQL("""
                SELECT category, COUNT(*) as count 
                FROM {}.{} 
                GROUP BY category 
                ORDER BY count DESC
            """).format(
                sql.Identifier(DB_SCHEMA),
                sql.Identifier(DB_TABLE)
            ))
            
            category_stats = cursor.fetchall()
            if category_stats:
                print("\nCurrent category distribution:")
                for category, count in category_stats:
                    print(f"  {category}: {count} ASINs")
            else:
                print("\nNo category data yet.")
        except Exception as e:
            print(f"\nNote: Could not fetch category stats: {e}")
        
    except Exception as e:
        print(f"Error creating schema/table: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def save_to_database(asins: List[str], domain_id: int, category: str):
    """Save ASINs to database with category."""
    if not asins:
        return 0
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        saved_count = 0
        error_count = 0
        
        batch_size = 100  # Process in batches for better performance
        
        for i in range(0, len(asins), batch_size):
            batch = asins[i:i + batch_size]
            
            # Prepare batch data - asin, amazon_url, and category
            batch_data = []
            for asin in batch:
                amazon_url = KeepaClient.product_link(asin, domain_id)
                batch_data.append((asin, amazon_url, category))
            
            try:
                # Use ON CONFLICT DO UPDATE to update category and timestamp
                insert_sql = sql.SQL("""
                INSERT INTO {}.{} (asin, amazon_url, category)
                VALUES %s
                ON CONFLICT (asin) DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP,
                    category = EXCLUDED.category
                RETURNING xmax::text::int > 0 as was_updated;
                """).format(
                    sql.Identifier(DB_SCHEMA),
                    sql.Identifier(DB_TABLE)
                )
                
                # Use psycopg2.extras.execute_values for batch insert
                from psycopg2.extras import execute_values
                results = execute_values(
                    cursor, 
                    insert_sql, 
                    batch_data,
                    template="(%s, %s, %s)",
                    fetch=True
                )
                
                # Count how many were inserted or updated
                saved_count += len(results)
                
            except Exception as e:
                print(f"Error in batch insert: {e}")
                # Try individual inserts for the failed batch
                conn.rollback()
                
                for asin, amazon_url, cat in batch_data:
                    try:
                        individual_insert_sql = sql.SQL("""
                        INSERT INTO {}.{} (asin, amazon_url, category)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (asin) DO UPDATE SET
                            updated_at = CURRENT_TIMESTAMP,
                            category = EXCLUDED.category;
                        """).format(
                            sql.Identifier(DB_SCHEMA),
                            sql.Identifier(DB_TABLE)
                        )
                        
                        cursor.execute(individual_insert_sql, (asin, amazon_url, cat))
                        if cursor.rowcount > 0:
                            saved_count += 1
                        
                    except Exception as inner_e:
                        print(f"Failed to save ASIN {asin}: {inner_e}")
                        error_count += 1
                        continue
                
                conn.commit()
                # Reconnect for next batch
                conn = get_db_connection()
                cursor = conn.cursor()
        
        conn.commit()
        
        print(f"  Database: Processed {saved_count} ASINs, Errors: {error_count}")
        
        return saved_count
        
    except Exception as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def check_existing_asins(asins: List[str], category: str = None) -> Dict[str, bool]:
    """Check which ASINs already exist in the database for a specific category."""
    if not asins:
        return {}
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        existing_asins = {}
        
        # Check in batches to avoid too many parameters
        batch_size = 500
        for i in range(0, len(asins), batch_size):
            batch = asins[i:i + batch_size]
            placeholders = ','.join(['%s'] * len(batch))
            
            if category:
                check_sql = sql.SQL("""
                SELECT asin FROM {}.{} 
                WHERE asin IN ({}) AND category = %s
                """).format(
                    sql.Identifier(DB_SCHEMA),
                    sql.Identifier(DB_TABLE),
                    sql.SQL(placeholders)
                )
                cursor.execute(check_sql, batch + [category])
            else:
                check_sql = sql.SQL("""
                SELECT asin FROM {}.{} 
                WHERE asin IN ({})
                """).format(
                    sql.Identifier(DB_SCHEMA),
                    sql.Identifier(DB_TABLE),
                    sql.SQL(placeholders)
                )
                cursor.execute(check_sql, batch)
            
            for row in cursor.fetchall():
                existing_asins[row[0]] = True
        
        return existing_asins
        
    except Exception as e:
        print(f"Error checking existing ASINs: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def get_database_stats():
    """Get statistics from the database."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Overall stats
        stats_sql = sql.SQL("""
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT category) as categories,
            MIN(updated_at) as oldest,
            MAX(updated_at) as newest,
            COUNT(DISTINCT DATE(updated_at)) as days_collected
        FROM {}.{};
        """).format(
            sql.Identifier(DB_SCHEMA),
            sql.Identifier(DB_TABLE)
        )
        
        cursor.execute(stats_sql)
        stats = cursor.fetchone()
        
        # Category breakdown
        category_sql = sql.SQL("""
        SELECT 
            category,
            COUNT(*) as count,
            MAX(updated_at) as last_updated
        FROM {}.{}
        GROUP BY category
        ORDER BY count DESC;
        """).format(
            sql.Identifier(DB_SCHEMA),
            sql.Identifier(DB_TABLE)
        )
        
        cursor.execute(category_sql)
        category_stats = cursor.fetchall()
        
        return {
            'total_records': stats[0],
            'categories_count': stats[1],
            'oldest_record': stats[2],
            'newest_record': stats[3],
            'days_collected': stats[4],
            'category_stats': [
                {
                    'category': row[0],
                    'count': row[1],
                    'last_updated': row[2]
                }
                for row in category_stats
            ]
        }
        
    except Exception as e:
        print(f"Error getting database stats: {e}")
        return {}
    finally:
        if conn:
            conn.close()


# ===== RATE LIMITER =====
class RateLimiter:
    def __init__(self, per_minute: int, per_hour: int):
        self.per_minute = per_minute
        self.per_hour = per_hour
        self.min_window = deque()
        self.hr_window = deque()

    def _evict(self, now: float):
        while self.min_window and now - self.min_window[0] >= 60:
            self.min_window.popleft()
        while self.hr_window and now - self.hr_window[0] >= 3600:
            self.hr_window.popleft()

    def acquire(self):
        while True:
            now = time.time()
            self._evict(now)
            if len(self.min_window) < self.per_minute and len(self.hr_window) < self.per_hour:
                self.min_window.append(now)
                self.hr_window.append(now)
                return
            next_min = 60 - (now - self.min_window[0]) if self.min_window else 0.05
            next_hr = 3600 - (now - self.hr_window[0]) if self.hr_window else 0.05
            time.sleep(max(0.05, min(next_min, next_hr)))


# ===== KEEPA CLIENT =====
class KeepaClient:
    def __init__(self, api_key: str):
        self.api_key = api_key.strip()
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": f"keepa-downloader/1.0 (SHARD_FIELD={SHARD_FIELD})"})
        self.rate = RateLimiter(REQS_PER_MINUTE, REQS_PER_HOUR)
        self._last_token_check = 0.0
        self._cached_tokens: Optional[int] = None

    def _ensure_tokens(self):
        now = time.time()
        if self._cached_tokens is None or (now - self._last_token_check) > TOKENS_POLL_SEC:
            self.rate.acquire()
            data = self._get_raw("token", {})
            self._cached_tokens = int(data.get("tokensLeft", 0))
            self._last_token_check = now

        while self._cached_tokens is not None and self._cached_tokens <= TOKENS_RESERVE:
            print(f"Tokens low ({self._cached_tokens} ≤ {TOKENS_RESERVE}). Waiting...")
            time.sleep(TOKENS_POLL_SEC)
            self.rate.acquire()
            data = self._get_raw("token", {})
            self._cached_tokens = int(data.get("tokensLeft", 0))
            self._last_token_check = time.time()

    def _raise_keepa(self, resp: requests.Response) -> None:
        try:
            data = resp.json()
        except Exception:
            resp.raise_for_status()
            return
        if "error" in data and data["error"]:
            raise RuntimeError(f"Keepa error: {data['error']}")
        resp.raise_for_status()

    def _get_raw(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"https://api.keepa.com/{endpoint.lstrip('/')}"
        r = self.s.get(url, params={"key": self.api_key, **params}, timeout=60)
        self._raise_keepa(r)
        self._cached_tokens = None
        return r.json()

    def _post_raw(self, endpoint: str, payload: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"https://api.keepa.com/{endpoint.lstrip('/')}"
        r = self.s.post(url, params={"key": self.api_key, **params}, json=payload, timeout=120)
        self._raise_keepa(r)
        self._cached_tokens = None
        return r.json()

    def _get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_tokens()
        self.rate.acquire()
        return self._get_raw(endpoint, params)

    def _post(self, endpoint: str, payload: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_tokens()
        self.rate.acquire()
        return self._post_raw(endpoint, payload, params)

    @staticmethod
    def decode_selection_from_url(url: str) -> Tuple[int, Dict[str, Any]]:
        parsed = up.urlparse(url)
        qs = up.parse_qs(parsed.query)
        try:
            domain = int(qs.get("domain", [1])[0])
        except ValueError:
            domain = 1
        sel_raw = qs.get("selection", ["{}"])[0]
        sel_dec = up.unquote(sel_raw)
        selection = json.loads(sel_dec)
        return domain, selection

    @staticmethod
    def product_link(asin: str, domain_id: int) -> str:
        base = {
            1: "https://www.amazon.com/dp/",
            2: "https://www.amazon.co.uk/dp/",
            3: "https://www.amazon.de/dp/",
            4: "https://www.amazon.fr/dp/",
            5: "https://www.amazon.co.jp/dp/",
            6: "https://www.amazon.ca/dp/",
            8: "https://www.amazon.it/dp/",
            9: "https://www.amazon.es/dp/",
            10: "https://www.amazon.in/dp/",
            11: "https://www.amazon.com.mx/dp/",
        }.get(domain_id, "https://www.amazon.com/dp/")
        return base + asin

    def tokens_left(self) -> int:
        data = self._get("token", {})
        return int(data.get("tokensLeft", 0))

    def fetch_query_post(self, selection: Dict[str, Any], domain_id: int = 1) -> Dict[str, Any]:
        return self._post("query", selection, {"domain": domain_id})


# ===== SHARDING =====
def shard_ranges(lo: int, hi: int, step: int):
    start = lo
    while start <= hi:
        end = min(start + step - 1, hi)
        yield (start, end)
        start = end + 1


# ===== CORE DOWNLOAD LOGIC =====
def process_single_shard(
    client: KeepaClient,
    domain_id: int,
    selection: Dict[str, Any],
    category: str,
    max_results: int = 1000
) -> List[str]:
    """Download ASINs from a single shard and save directly to database."""
    import math
    
    query = dict(selection)
    
    # Set sort order
    if PAGING_ORDER.lower() == "newest":
        query["sort"] = [["trackingSince", "desc"]]
    else:
        query["sort"] = [["trackingSince", "asc"]]
    
    # Snapshot cutoff for consistent data
    if SNAPSHOT_FREEZE:
        cutoff = int(time.time())
        if PAGING_ORDER.lower() == "newest":
            query["trackingSince_lte"] = cutoff
        else:
            query["trackingSince_lte"] = cutoff
    
    results: List[str] = []
    page = 0
    per_page = 100
    
    while True:
        query["page"] = page
        query["perPage"] = per_page
        
        try:
            data = client.fetch_query_post(query, domain_id=domain_id)
        except RuntimeError as e:
            msg = str(e)
            if "perPage" in msg and "page" in msg:
                new_pp = max(50, math.floor(per_page / 2))
                if new_pp == per_page:
                    raise
                print(f"  Keepa rejected perPage={per_page}. Backing off to {new_pp}...")
                per_page = new_pp
                continue
            raise
        
        total_results = data.get("totalResults")
        if total_results is not None and page == 0:
            print(f"  Total results: {total_results}")
        
        batch = data.get("asinList", []) or []
        
        # Check for duplicates in this batch before saving
        new_asins = []
        for a in batch:
            if a not in results:  # Local duplicate check within this shard
                results.append(a)
                new_asins.append(a)
        
        # Save to database with category
        if new_asins:
            print(f"  Saving {len(new_asins)} ASINs to database...")
            saved = save_to_database(new_asins, domain_id, category)
            if saved > 0:
                print(f"  ✓ Successfully saved {saved} ASINs for category: {category}")
        
        # Check if we should continue
        if len(batch) >= per_page:
            if page >= 9:  # Keepa page limit
                print(f"  ⚠️  Reached page limit (page {page})")
                break
            page += 1
            continue
        
        break
    
    print(f"  Processed {len(results)} ASINs for category: {category}")
    return results


def check_shard_size(client: KeepaClient, domain_id: int, selection: Dict[str, Any]) -> int:
    """Quick check of shard size."""
    query = dict(selection)
    query["page"] = 0
    query["perPage"] = 100
    if PAGING_ORDER.lower() == "newest":
        query["sort"] = [["trackingSince", "desc"]]
    else:
        query["sort"] = [["trackingSince", "asc"]]
    
    try:
        data = client.fetch_query_post(query, domain_id=domain_id)
        return data.get("totalResults", 0)
    except Exception as e:
        print(f"  Warning: Could not check shard size: {e}")
        return 0


def process_shard_adaptive(
    client: KeepaClient,
    domain_id: int,
    base_selection: Dict[str, Any],
    category: str,
    lo: int,
    hi: int,
    max_results: int = 1000
) -> List[str]:
    """Process a shard, subdividing if too many results."""
    
    # Build selection for this shard
    sel = dict(base_selection)
    for k in (f"{SHARD_FIELD}_gte", f"{SHARD_FIELD}_lte", "current_SALES_gte", "current_SALES_lte",
              "trackingSince_gte", "trackingSince_lte", "page"):
        sel.pop(k, None)
    sel[f"{SHARD_FIELD}_gte"] = lo
    sel[f"{SHARD_FIELD}_lte"] = hi
    
    # Check how many results this shard has
    print(f"\n=== Checking Shard {SHARD_FIELD} {lo}..{hi} ===")
    total_results = check_shard_size(client, domain_id, sel)
    print(f"  Total results: {total_results}")
    
    mid = (lo + hi) // 2
    if total_results > max_results:
        if mid == lo:  
            print(f"  ⚠️  Cannot subdivide further, processing anyway...")
        else:
            print(f"  📊 Subdividing into {lo}..{mid} and {mid+1}..{hi}")
            # Recurse into sub-shards
            results1 = process_shard_adaptive(client, domain_id, base_selection, category, lo, mid, max_results)
            results2 = process_shard_adaptive(client, domain_id, base_selection, category, mid+1, hi, max_results)
            return results1 + results2
    
    # Process this shard
    print(f"  ✓ Processing shard {lo}..{hi}")
    results = process_single_shard(client, domain_id, sel, category, max_results)
    
    return results


def process_category(client: KeepaClient, category_info: dict):
    """Process a single category."""
    category_name = category_info['name']
    url = category_info['url']
    
    print(f"\n{'='*60}")
    print(f"Processing Category: {category_name}")
    print(f"{'='*60}")
    
    # Extract API key from URL if not already extracted
    if not client.api_key and category_info.get('api_key'):
        client.api_key = category_info['api_key']
    
    try:
        # Decode the URL to get selection
        domain_id, selection = KeepaClient.decode_selection_from_url(url)
        
        total_asins = []
        start_time = time.time()
        
        # Get shard range from selection
        lo_key = f"{SHARD_FIELD}_gte"
        hi_key = f"{SHARD_FIELD}_lte"
        shard_min = int(selection.get(lo_key, 1))
        
        # Extract max sales from the URL selection
        shard_max = int(selection.get(hi_key, 2_000_000))
        
        print(f"Domain ID: {domain_id}")
        print(f"Shard range: {shard_min} to {shard_max}")
        print(f"Shard size: {SHARD_SIZE}")
        print(f"Shard field: {SHARD_FIELD}")
        
        # Check initial tokens
        try:
            tokens = client.tokens_left()
            print(f"Tokens available: {tokens}")
        except:
            print("Could not check token count")
        
        # Process each shard
        for lo, hi in shard_ranges(shard_min, shard_max, SHARD_SIZE):
            shard_results = process_shard_adaptive(
                client, domain_id, selection, category_name, lo, hi
            )
            total_asins.extend(shard_results)
        
        # Count unique ASINs
        unique_asins = list(dict.fromkeys(total_asins))
        elapsed_time = time.time() - start_time
        
        print(f"\n{'='*60}")
        print(f"✅ COMPLETED: {category_name}")
        print(f"{'='*60}")
        print(f"Total ASINs processed: {len(total_asins)}")
        print(f"Unique ASINs: {len(unique_asins)}")
        print(f"Time taken: {elapsed_time:.2f} seconds")
        
        return len(unique_asins)
        
    except Exception as e:
        print(f"❌ Error processing category {category_name}: {e}")
        return 0


# ===== MAIN FUNCTION =====
def main():
    """Main function - processes all categories from .env file."""
    
    print("=" * 70)
    print("KEEPA SCRAPER - MULTI-CATEGORY DATABASE STORAGE")
    print("=" * 70)
    
    # Get categories from .env
    categories = get_categories_from_env()
    
    if not categories:
        print("No categories found. Exiting.")
        return
    
    print(f"\nFound {len(categories)} categories to process")
    
    # Initialize database schema and table
    print("\nInitializing database...")
    try:
        create_schema_and_table()
        db_enabled = True
    except Exception as e:
        print(f"Database initialization failed: {e}")
        print("Cannot continue without database connection.")
        return
    
    # Get API key from first category (all should have same key)
    first_api_key = categories[0].get('api_key')
    if not first_api_key:
        print("❌ No API key found in category URLs")
        print("Make sure your .env URLs contain the key= parameter")
        return
    
    # Initialize Keepa client with API key from first category
    client = KeepaClient(first_api_key)
    
    # Check initial tokens
    try:
        tokens = client.tokens_left()
        print(f"✓ Keepa tokens available: {tokens}")
    except Exception as e:
        print(f"Warning: token check failed: {e}")
    
    # Process each category
    print(f"\n{'='*70}")
    print(f"STARTING PROCESSING OF {len(categories)} CATEGORIES")
    print(f"{'='*70}")
    
    category_results = {}
    total_start_time = time.time()
    
    for i, category_info in enumerate(categories, 1):
        category_name = category_info['name']
        print(f"\n📦 Processing category {i}/{len(categories)}: {category_name}")
        
        asins_collected = process_category(client, category_info)
        category_results[category_name] = asins_collected
        
        # Wait a bit between categories (except after last one)
        if i < len(categories):
            wait_time = 30  # 30 seconds between categories
            print(f"\n⏳ Waiting {wait_time} seconds before next category...")
            time.sleep(wait_time)
    
    total_elapsed_time = time.time() - total_start_time
    
    # Final summary
    print(f"\n{'='*70}")
    print(f"🏁 ALL CATEGORIES COMPLETED")
    print(f"{'='*70}")
    
    total_asins = sum(category_results.values())
    print(f"\nSummary by category:")
    for category, count in category_results.items():
        print(f"  {category}: {count} ASINs")
    
    print(f"\nTotal ASINs collected: {total_asins}")
    print(f"Total time: {total_elapsed_time:.2f} seconds ({total_elapsed_time/60:.2f} minutes)")
    print(f"Database: {DB_SCHEMA}.{DB_TABLE}")
    
    # Show database statistics
    if db_enabled:
        stats = get_database_stats()
        if stats:
            print(f"\n📊 Database Statistics:")
            print(f"  Total records: {stats.get('total_records', 'N/A')}")
            print(f"  Categories: {stats.get('categories_count', 'N/A')}")
            print(f"  Oldest record: {stats.get('oldest_record', 'N/A')}")
            print(f"  Newest record: {stats.get('newest_record', 'N/A')}")
            print(f"  Days of collection: {stats.get('days_collected', 'N/A')}")
            
            if stats.get('category_stats'):
                print(f"\nCategory breakdown:")
                for cat_stat in stats['category_stats']:
                    print(f"  {cat_stat['category']}: {cat_stat['count']} ASINs (last: {cat_stat['last_updated']})")
    
    print(f"\n✅ All data stored in database with category information.")
    print(f"   Schema: {DB_SCHEMA}")
    print(f"   Table: {DB_TABLE}")
    print(f"   Columns: asin (PRIMARY KEY), amazon_url, category, updated_at")


if __name__ == "__main__":
    main()