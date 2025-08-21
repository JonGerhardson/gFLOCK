import os
import sqlite3
import traceback
import csv
from datetime import datetime
from bs4 import BeautifulSoup

# --- Configuration ---
DATABASE_NAME = 'agency_data.db'
ROOT_DIRECTORY = os.path.abspath('scraped_data')

def confirm_database_reset():
    """Safely handles database reset with user confirmation."""
    if os.path.exists(DATABASE_NAME):
        confirm = input(f"Database '{DATABASE_NAME}' exists. Overwrite? [y/N]: ").strip().lower()
        if confirm != 'y':
            print("Operation canceled.")
            return False
        try:
            os.remove(DATABASE_NAME)
            print("Removed existing database.")
        except OSError as e:
            print(f"Error removing database file: {e}")
            return False
    return True

def create_database_schema(cursor):
    """Creates the database tables with a more structured schema for parsed content."""
    print("Creating database schema...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS agencies (
            agency_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            state TEXT NOT NULL,
            UNIQUE(name, state)
        )
    ''')
    # This table now includes columns for data extracted from page_content.html
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scrapes (
            scrape_id INTEGER PRIMARY KEY AUTOINCREMENT,
            agency_id INTEGER NOT NULL,
            scrape_date TEXT NOT NULL,
            overview_text TEXT,
            vehicles_detected INTEGER,
            hotlist_hits INTEGER,
            searches_last_30_days INTEGER,
            UNIQUE(agency_id, scrape_date),
            FOREIGN KEY (agency_id) REFERENCES agencies (agency_id) ON DELETE CASCADE
        )
    ''')
    # This table will catalog all files, but not their content.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scrape_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_type TEXT,
            file_size INTEGER,
            FOREIGN KEY (scrape_id) REFERENCES scrapes (scrape_id) ON DELETE CASCADE
        )
    ''')
    # A new table specifically for the contents of search_audit.csv files.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS search_audits (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scrape_id INTEGER NOT NULL,
            search_guid TEXT,
            user_guid TEXT,
            search_timestamp TEXT,
            camera_count INTEGER,
            reason TEXT,
            FOREIGN KEY (scrape_id) REFERENCES scrapes (scrape_id) ON DELETE CASCADE
        )
    ''')
    conn.commit()
    print("Database schema created.")

def create_database_indexes(cursor):
    """Creates indexes after data insertion for better performance."""
    print("Creating database indexes...")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_agencies_state ON agencies(state)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_scrapes_date ON scrapes(scrape_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_type ON files(file_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_audits_scrape_id ON search_audits(scrape_id)')
    conn.commit()
    print("Database indexes created.")

def parse_html_content(file_path):
    """Parses key metrics from a page_content.html file using BeautifulSoup."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
        
        data = {
            'overview': None,
            'vehicles': None,
            'hits': None,
            'searches': None
        }

        # Extract overview text
        overview_div = soup.select_one('#overview div.box')
        if overview_div:
            data['overview'] = overview_div.get_text(strip=True)

        # Extract usage stats
        usage_boxes = soup.select('#usage div.box .value')
        for box in usage_boxes:
            label = box.find_previous_sibling('div', class_='label')
            if label:
                label_text = label.get_text(strip=True).lower()
                value_text = box.get_text(strip=True).replace(',', '')
                
                if 'unique vehicles' in label_text:
                    data['vehicles'] = int(value_text) if value_text.isdigit() else None
                elif 'hotlist hits' in label_text:
                    data['hits'] = int(value_text) if value_text.isdigit() else None
                elif 'searches' in label_text:
                    data['searches'] = int(value_text) if value_text.isdigit() else None
        
        return data
    except Exception as e:
        print(f"    - Could not parse HTML file {os.path.basename(file_path)}: {e}")
        return None

def process_directories(conn):
    """Walks directories, parses content from specific files, and populates the database."""
    cursor = conn.cursor()
    total_files_cataloged = 0
    total_audits_logged = 0

    for root, dirs, files_in_dir in os.walk(ROOT_DIRECTORY):
        rel_path = os.path.relpath(root, ROOT_DIRECTORY)
        path_parts = rel_path.split(os.sep)
        
        if len(path_parts) != 3:
            continue
            
        state, agency_name, date_str = path_parts
        
        if len(state) != 2 or not state.isupper(): continue
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            continue

        print(f"\nProcessing: {state} -> {agency_name} -> {date_str}")

        # --- 1. Get Agency and Scrape IDs ---
        cursor.execute("INSERT INTO agencies (name, state) VALUES (?, ?) ON CONFLICT(name, state) DO NOTHING", (agency_name, state))
        cursor.execute("SELECT agency_id FROM agencies WHERE name = ? AND state = ?", (agency_name, state))
        agency_id = cursor.fetchone()[0]
        
        cursor.execute("INSERT INTO scrapes (agency_id, scrape_date) VALUES (?, ?) ON CONFLICT(agency_id, scrape_date) DO NOTHING", (agency_id, date_str))
        cursor.execute("SELECT scrape_id FROM scrapes WHERE agency_id = ? AND scrape_date = ?", (agency_id, date_str))
        scrape_id = cursor.fetchone()[0]

        # --- 2. Process each file in the directory ---
        for file_name in files_in_dir:
            full_path = os.path.join(root, file_name)
            if not os.path.isfile(full_path): continue

            # --- 2a. Catalog every file's metadata ---
            file_ext = os.path.splitext(file_name)[1][1:] if '.' in file_name else None
            file_size = os.path.getsize(full_path)
            relative_path = os.path.relpath(full_path, os.path.dirname(ROOT_DIRECTORY))
            cursor.execute(
                "INSERT INTO files (scrape_id, file_name, file_path, file_type, file_size) VALUES (?, ?, ?, ?, ?)",
                (scrape_id, file_name, relative_path, file_ext, file_size)
            )
            total_files_cataloged += 1

            # --- 2b. Parse content from page_content.html ---
            if file_name.lower() == 'page_content.html':
                html_data = parse_html_content(full_path)
                if html_data:
                    cursor.execute(
                        """UPDATE scrapes SET overview_text = ?, vehicles_detected = ?, hotlist_hits = ?, searches_last_30_days = ?
                           WHERE scrape_id = ?""",
                        (html_data['overview'], html_data['vehicles'], html_data['hits'], html_data['searches'], scrape_id)
                    )
                    print("    - Parsed and stored content from page_content.html")

            # --- 2c. Parse content from search_audit.csv ---
            elif file_name.lower() == 'search_audit.csv':
                try:
                    with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                        reader = csv.reader(f)
                        header = next(reader) # Skip header
                        audits_to_add = []
                        for row in reader:
                            # Ensure row has the correct number of columns
                            if len(row) == 5:
                                audits_to_add.append((scrape_id, row[0], row[1], row[2], int(row[3]) if row[3].isdigit() else None, row[4]))
                    
                    if audits_to_add:
                        cursor.executemany(
                            "INSERT INTO search_audits (scrape_id, search_guid, user_guid, search_timestamp, camera_count, reason) VALUES (?, ?, ?, ?, ?, ?)",
                            audits_to_add
                        )
                        print(f"    - Logged {len(audits_to_add)} records from search_audit.csv")
                        total_audits_logged += len(audits_to_add)
                except Exception as e:
                    print(f"    - Could not process CSV file {file_name}: {e}")
        
        conn.commit()

    return total_files_cataloged, total_audits_logged

def main():
    """Main execution flow."""
    if not confirm_database_reset():
        return
    
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), DATABASE_NAME)

    global conn
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA foreign_keys = ON;")
            
            cursor = conn.cursor()
            create_database_schema(cursor)
            
            print("\nStarting directory processing...")
            files_count, audits_count = process_directories(conn)
            
            create_database_indexes(cursor)
            
            print(f"\nSuccess! Cataloged {files_count:,} files and logged {audits_count:,} audit records.")
            db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
            print(f"Final database size: {db_size_mb:.2f} MB")
    
    except sqlite3.Error as e:
        print(f"\nAn SQLite database error occurred: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        print("\nOperation finished.")

if __name__ == '__main__':
    main()

