from flask import Flask, request, jsonify
import pandas as pd
import requests

# --- CONFIG ---
LOCAL_DATA_PATH = "processed_data/player_data_clean.csv"
BALlDONTLIE_API_URL = "https://api.balldontlie.io/v1/players"
API_KEY = "bb521bdf-3307-43c2-b2be-42ab9c813dd6"  # Same as your ETL key

# --- FLASK APP ---
app = Flask(__name__)

# Load local player data once at startup
try:
    player_data = pd.read_csv(LOCAL_DATA_PATH)
except FileNotFoundError:
    print(f"Error: {LOCAL_DATA_PATH} not found. Make sure to run the ETL first.")
    player_data = pd.DataFrame()

# --- HELPER FUNCTIONS ---
def find_local_player(player_name):
    """
    Find player info in local CSV.
    """
    matches = player_data[player_data["player_name"].str.contains(player_name, case=False, na=False)]
    if matches.empty:
        return None
    return matches.iloc[0].to_dict()

def find_api_player(player_name):
    """
    Search player info via balldontlie API.
    """
    headers = {"Authorization": f"Bearer {API_KEY}"}
    params = {"search": player_name}
    try:
        response = requests.get(BALlDONTLIE_API_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if data["data"]:
            return data["data"][0]  # Return first match
    except requests.RequestException as e:
        print(f"API error: {e}")
    return None

# --- ROUTES ---
@app.route("/", methods=["GET"])
def home():
    return "🏀 Flask Chatbot is running! POST to /chat with JSON."
@app.route("/chat", methods=["GET", "POST"])
def chat():
    """
    Chatbot endpoint. Expects JSON {"message": "..."}
    """
    if request.method == "GET":
        return "<h1>POST JSON to this endpoint, e.g. {message: ...}</h1>"
    data = request.get_json()
    if not data or "message" not in data:
        return jsonify({"error": "Invalid request, expected JSON with 'message' field."}), 400

    message = data["message"].strip()
    if not message:
        return jsonify({"error": "Message cannot be empty."}), 400

    # Very simple logic: try to find a player
    player_info = find_local_player(message)

    if player_info:
        response = (f"{player_info['player_name']} played {player_info['primary_position']} position. "
                    f"They were {player_info['height_in']} inches tall, "
                    f"weighed {int(player_info['weight_lbs'])} lbs, "
                    f"and went to {player_info['college'] if pd.notna(player_info['college']) else 'unknown college'}.")
    else:
        # Try the API
        api_player = find_api_player(message)
        if api_player:
            response = (f"{api_player['first_name']} {api_player['last_name']} currently plays for "
                        f"{api_player['team']['full_name']} as a {api_player['position']} (API data).")
        else:
            response = f"Sorry, I couldn't find any information about '{message}'."

    return jsonify({"response": response})

# --- MAIN ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
