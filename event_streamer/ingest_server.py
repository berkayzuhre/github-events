import os
import threading
import time
import requests
import psycopg2
import json
from flask import Flask

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", 10))  # seconds

PG_CONN = {
    'dbname': os.environ["PGDATABASE"],
    'user': os.environ["PGUSER"],
    'password': os.environ["PGPASSWORD"],
    'host': os.environ["PGHOST"],
    'port': os.environ.get("PGPORT", 5432),
}

# Table is now bronze_github_events
def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze_github_events (
            id TEXT PRIMARY KEY,
            type TEXT,
            repo_name TEXT,
            actor_login TEXT,
            raw JSONB,
            created_at TIMESTAMPTZ
        );
        """)
        conn.commit()

def save_events(conn, events):
    with conn.cursor() as cur:
        for e in events:
            event_id = e["id"]
            event_type = e.get("type")
            repo = e.get("repo", {}).get("name")
            actor = e.get("actor", {}).get("login")
            created_at = e.get("created_at")
            raw = json.dumps(e)
            cur.execute("""
            INSERT INTO bronze_github_events (id, type, repo_name, actor_login, raw, created_at)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (id) DO NOTHING
            """, (event_id, event_type, repo, actor, raw, created_at))
        conn.commit()

def get_next_link(link_header):
    if not link_header:
        return None
    parts = link_header.split(',')
    for part in parts:
        url_part, rel_part = part.split(';')
        url = url_part.strip()[1:-1]  # removes < and >
        rel = rel_part.strip()
        if rel == 'rel="next"':
            return url
    return None


def fetch_events(max_pages=2, per_page=100):
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    events = []
    url = f"https://api.github.com/events?per_page={per_page}"
    pages_fetched = 0

    while url and pages_fetched < max_pages:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            raise Exception(f"GitHub API error: {resp.status_code}, {resp.text}")

        batch = resp.json()
        events.extend(batch)
        pages_fetched += 1

        # Get "next" URL from Link header for pagination
        link = resp.headers.get("Link")
        url = None  # clear for next iteration
        if link:
            # Search Link header for rel="next"
            next_url = get_next_link(link)
            if next_url:
                url = next_url  # go to next page
        print(f"Fetched {len(batch)} events from page {pages_fetched}")

    print(f"Fetched total {len(events)} events in {pages_fetched} pages")
    return events

def ingest_loop():
    conn = None
    while True:
        try:
            if conn is None or conn.closed:
                conn = psycopg2.connect(**PG_CONN)
                create_table_if_not_exists(conn)
            events = fetch_events()
            if events:
                save_events(conn, events)
                print(f"Inserted {len(events)} new events.")
            else:
                print("No events this poll.")
        except Exception as ex:
            print(f"Error in ingest loop: {ex}")
        time.sleep(POLL_INTERVAL)

app = Flask(__name__)

@app.route('/')
def health():
    return "OK"

if __name__ == '__main__':
    t = threading.Thread(target=ingest_loop, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)