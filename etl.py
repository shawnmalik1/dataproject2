import os
import time
import pandas as pd
from balldontlie import BalldontlieAPI
from balldontlie.base import RateLimitError

# Ensure directories exist
os.makedirs("raw_data", exist_ok=True)
os.makedirs("processed_data", exist_ok=True)

# --- EXTRACT CSV DATA (ONLY player_data.csv) ---
def extract_player_csv():
    pdata_df = pd.read_csv("player_data.csv")
    pdata_df.to_csv("raw_data/player_data_raw.csv", index=False)
    return pdata_df

# --- TRANSFORM PLAYER DATA ---
def _parse_height(h: str) -> int:
    if pd.isna(h) or '-' not in str(h):
        return None
    ft, inch = h.split('-')
    try:
        return int(ft) * 12 + int(inch)
    except:
        return None

def transform_player_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "name":       "player_name",
        "position":   "primary_position",
        "height":     "height_str",
        "weight":     "weight_lbs",
        "birth_date": "birth_date",
        "college":    "college"
    })
    df["height_in"]  = df["height_str"].apply(_parse_height)
    df["weight_lbs"] = pd.to_numeric(df["weight_lbs"], errors="coerce")
    df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce")
    return df.drop(columns=["height_str"]).drop_duplicates(subset=["player_name"])

# --- EXTRACT + TRANSFORM API PLAYER DATA ---
def extract_api_players(api_key: str, per_page: int = 25) -> pd.DataFrame:
    """
    Pages through the balldontlie API (with backoff on 429s),
    archives raw JSON → CSV, normalizes into a flat table,
    guarantees expected fields, renames for consistency,
    drops duplicates, writes cleaned CSV, and returns it.
    """
    # Ensure output dirs exist
    os.makedirs("raw_data", exist_ok=True)
    os.makedirs("processed_data", exist_ok=True)

    api = BalldontlieAPI(api_key=api_key)
    all_records = []
    resp = None

    # --- first page with backoff ---
    while True:
        try:
            resp = api.nba.players.list(per_page=per_page,
                                        cursor=(resp.meta.next_cursor if resp else None))
            break
        except RateLimitError:
            print("[RATE LIMIT] sleeping 60s before retry...")
            time.sleep(60)

    # resp.dict() gives {'data': [...], 'meta': {...}}
    all_records.extend(resp.dict()["data"])
    print(f"[API] fetched {len(resp.dict()['data'])} players (cursor={resp.meta.next_cursor})")

    # --- subsequent pages ---
    while resp.meta.next_cursor:
        while True:
            try:
                resp = api.nba.players.list(per_page=per_page,
                                            cursor=resp.meta.next_cursor)
                break
            except RateLimitError:
                print("[RATE LIMIT] sleeping 60s before retry...")
                time.sleep(60)

        batch = resp.dict()["data"]
        all_records.extend(batch)
        print(f"[API] fetched {len(batch)} players (cursor={resp.meta.next_cursor})")

    # --- normalize & archive raw ---
    raw_df = pd.json_normalize(all_records, sep="_")
    raw_path = "raw_data/players_api_raw.csv"
    raw_df.to_csv(raw_path, index=False)
    print(f"[ARCHIVE] raw API data → {raw_path} ({len(raw_df)} rows)")

    # --- guarantee expected fields ---
    expected = [
        "id", "first_name", "last_name", "position", "height", "weight", "jersey_number",
        "college", "country", "draft_year", "draft_round", "draft_number",
        "team_id", "team_full_name", "team_abbreviation"
    ]
    for col in expected:
        if col not in raw_df.columns:
            raw_df[col] = None

    # --- rename for consistency & select ---
    rename_map = {
        "id": "player_id",
        "first_name": "first",
        "last_name": "last",
        "position": "position",
        "height": "height_str",
        "weight": "weight_lbs",
        "jersey_number": "jersey",
        "college": "college",
        "country": "country",
        "draft_year": "draft_year",
        "draft_round": "draft_round",
        "draft_number": "draft_number",
        "team_id": "team_id",
        "team_full_name": "team_full_name",
        "team_abbreviation": "team_abbr"
    }
    df = raw_df.rename(columns=rename_map)

    keep_cols = list(rename_map.values())
    clean_df = df[keep_cols].drop_duplicates()

    # --- write cleaned CSV ---
    clean_path = "processed_data/players_api_clean.csv"
    clean_df.to_csv(clean_path, index=False)
    print(f"[CLEAN] cleaned API data → {clean_path} ({len(clean_df)} rows)")

    return clean_df


# --- LOAD FUNCTION ---
def load_df(df: pd.DataFrame, filename: str):
    path = f"processed_data/{filename}"
    df.to_csv(path, index=False)
    print(f"[LOAD] → {path} ({len(df)} rows)")

# --- ORCHESTRATOR ---
def run_etl():
    print("Starting ETL pipeline...")

    # Step 1: Extract and Transform player_data.csv
    pdata_raw = extract_player_csv()
    pdata_clean = transform_player_data(pdata_raw)
    print("Processed player CSV data.")

    # Step 2: Extract and Transform API players
    api_clean = extract_api_players("bb521bdf-3307-43c2-b2be-42ab9c813dd6")
    print("Processed API player data.")

    # Step 3: Load
    load_df(pdata_clean, "player_data_clean.csv")
    load_df(api_clean, "players_api_clean.csv")

    print("ETL pipeline completed successfully.")

if __name__ == "__main__":
    run_etl()