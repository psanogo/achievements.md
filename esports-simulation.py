import json
import random
import time
from datetime import datetime, timezone
import uuid
from google.cloud import pubsub_v1

# --- Configuration ---
# TODO: Replace with your Google Cloud Project ID
PROJECT_ID = "qwiklabs-gcp-03-cac5a63795c8"
# TODO: Replace with your new Pub/Sub Topic ID for e-sports
TOPIC_ID = "esports_events_topic"

# --- E-sports Simulation Data ---
TEAMS = {
    "team_alpha": ["alpha_player1", "alpha_player2", "alpha_player3"],
    "team_bravo": ["bravo_player1", "bravo_player2", "bravo_player3"],
    "team_charli": ["charli_player1", "charli_player2", "charli_player3"],
    "team_delta": ["delta_player1", "delta_player2", "delta_player3"],
}
GAME_NAMES = ["Cosmic Clash", "Starfire Strike", "Quantum Arena"]

# --- Pub/Sub Publisher Client ---
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

print(f"Publisher configured to send messages to: {topic_path}")
print("Generating and publishing e-sports match events...")
print("Press Ctrl+C to stop.")

def generate_event(match_id, game_name, event_type, team1, player1, team2, player2, winner_info=None):
    """Constructs a standard event dictionary."""
    event = {
        "event_id": str(uuid.uuid4()),
        "match_id": match_id,
        "game_name": game_name,
        "event_type": event_type,
        "team_a_id": team1,
        "player_a_id": player1,
        "team_b_id": team2,
        "player_b_id": player2,
        "winner_player_id": None,
        "winner_team_id": None,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    if winner_info:
        event["winner_player_id"] = winner_info.get("player")
        event["winner_team_id"] = winner_info.get("team")
    return event

def publish_event(event):
    """Publishes a single event to Pub/Sub."""
    json_message = json.dumps(event)
    data = json_message.encode("utf-8")
    future = publisher.publish(topic_path, data, match_id=event['match_id'])
    message_id = future.result()
    print(f"Published Event: {event['event_type']} for Match: {event['match_id']}. Msg ID: {message_id}")

try:
    while True:
        # 1. Setup a new match
        match_id = str(uuid.uuid4())
        game_name = random.choice(GAME_NAMES)
        team1_id, team2_id = random.sample(list(TEAMS.keys()), 2)
        player1_id = random.choice(TEAMS[team1_id])
        player2_id = random.choice(TEAMS[team2_id])

        # 2. Publish a 'match_start' event
        start_event = generate_event(match_id, game_name, "match_start", team1_id, player1_id, team2_id, player2_id)
        publish_event(start_event)
        time.sleep(1)

        # 3. Simulate an elimination
        # For simplicity, we'll just have one player eliminate the other.
        # In a real scenario, you might have multiple eliminations.
        winner, loser = random.sample([(player1_id, team1_id), (player2_id, team2_id)], 2)
        winner_player, winner_team = winner
        loser_player, loser_team = loser
        
        elimination_event = generate_event(match_id, game_name, "player_elimination", team1_id, player1_id, team2_id, player2_id,
                                           winner_info={"player": winner_player, "team": winner_team})
        publish_event(elimination_event)
        time.sleep(1)
        
        # 4. Publish a 'match_end' event
        end_event = generate_event(match_id, game_name, "match_end", team1_id, player1_id, team2_id, player2_id,
                                   winner_info={"player": winner_player, "team": winner_team})
        publish_event(end_event)

        # 5. Wait before starting the next match
        print(f"--- Match {match_id} Concluded. Winner: {winner_player} of {winner_team}. ---\n")
        time.sleep(5)

except KeyboardInterrupt:
    print("\nStopping event generation.")
except Exception as e:
    print(f"An error occurred: {e}")
