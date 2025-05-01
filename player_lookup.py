# player_lookup.py
import pandas as pd
import requests

LOCAL_DATA_PATH = "processed_data/player_data_clean.csv"
BALlDONTLIE_API_URL = "https://api.balldontlie.io/v1/players"
API_KEY = "bb521bdf-3307-43c2-b2be-42ab9c813dd6"

try:
    player_data = pd.read_csv(LOCAL_DATA_PATH)
except FileNotFoundError:
    print("Data file not found.")
    player_data = pd.DataFrame()

def find_local_player(player_name):
    matches = player_data[player_data["player_name"].str.contains(player_name, case=False, na=False)]
    return matches.iloc[0].to_dict() if not matches.empty else None

def find_api_player(player_name):
    headers = {"Authorization": f"Bearer {API_KEY}"}
    params = {"search": player_name}
    try:
        response = requests.get(BALlDONTLIE_API_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return data["data"][0] if data["data"] else None
    except:
        return None

def generate_player_response(name):
    player = find_local_player(name)
    if player:
        return (f"{player['player_name']} played {player['primary_position']} position. "
                f"They were {player['height_in']} inches tall, weighed {int(player['weight_lbs'])} lbs, "
                f"and went to {player['college'] if pd.notna(player['college']) else 'unknown college'}.")
    else:
        api_player = find_api_player(name)
        if api_player:
            return (f"{api_player['first_name']} {api_player['last_name']} currently plays for "
                    f"{api_player['team']['full_name']} as a {api_player['position']} (API data).")
        return f"Sorry, I couldn't find any information about '{name}'."
