import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
import json
import datetime
import logging

# --- Configuration ---
# TODO: Replace with your Google Cloud Project ID
PROJECT_ID = "qwiklabs-gcp-03-cac5a63795c8"
PUBSUB_SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/esports_events_topic-sub"
BIGQUERY_DATASET = "esports_analytics"
BIGQUERY_RAW_TABLE = f"{PROJECT_ID}:{BIGQUERY_DATASET}.raw_events"
BIGQUERY_PLAYER_SCORE_UPDATES = f"{PROJECT_ID}:{BIGQUERY_DATASET}.player_score_updates"
BIGQUERY_TEAM_SCORE_UPDATES = f"{PROJECT_ID}:{BIGQUERY_DATASET}.team_score_updates"
GCS_TEMP_LOCATION = f"gs://{PROJECT_ID}-bucket/temp"
REGION = "us-central1"


# --- PTransforms ---

class ParseEvent(beam.DoFn):
    def process(self, element):
        try:
            yield json.loads(element.decode('utf-8'))
        except Exception as e:
            logging.error(f"Could not parse JSON message: {element}. Error: {e}")

class CalculatePlayerScores(beam.DoFn):
    def process(self, element):
        event_type = element.get('event_type')
        winner_player = element.get('winner_player_id')
        if winner_player and event_type in ['player_elimination', 'match_end']:
            score = 5 if event_type == 'match_end' else 1
            yield (winner_player, score)

class CalculateTeamScores(beam.DoFn):
    def process(self, element):
        event_type = element.get('event_type')
        winner_team = element.get('winner_team_id')
        if winner_team and event_type == 'match_end':
            yield (winner_team, 1)

class UpdateScoreWithState(beam.DoFn):
    SCORE_STATE = ReadModifyWriteStateSpec('score', beam.coders.VarIntCoder())

    def process(self, element, score_state=beam.DoFn.StateParam(SCORE_STATE)):
        key, scores = element
        current_score = score_state.read() or 0
        total_score = current_score + sum(scores)
        score_state.write(total_score)
        yield (key, total_score)

class FormatScoreUpdate(beam.DoFn):
    """Formats the (key, score) tuple into a BigQuery dictionary."""
    def process(self, element, id_field, score_field):
        key, score = element
        yield {
            id_field: key,
            score_field: score,
            'last_updated': datetime.datetime.now(datetime.timezone.utc).isoformat()
        }

# --- Pipeline Definition ---

def run():
    options = PipelineOptions(
        flags=[],
        project=PROJECT_ID,
        region=REGION,
        temp_location=GCS_TEMP_LOCATION,
        runner='DataflowRunner',
        streaming=True,
        job_name='esports-leaderboard-pipeline-views-v1',
        experiments=['shuffle_mode=service']
    )

    raw_schema_string = 'event_id:STRING, match_id:STRING, game_name:STRING, event_type:STRING, team_a_id:STRING, player_a_id:STRING, team_b_id:STRING, player_b_id:STRING, winner_player_id:STRING, winner_team_id:STRING, timestamp:TIMESTAMP'
    player_score_schema = 'player_id:STRING, total_score:INTEGER, last_updated:TIMESTAMP'
    team_score_schema = 'team_id:STRING, total_wins:INTEGER, last_updated:TIMESTAMP'

    with beam.Pipeline(options=options) as p:
        parsed_events = (
            p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
              | 'ParseJsonEvents' >> beam.ParDo(ParseEvent())
        )

        # Branch 1: Write Raw Events (Unchanged)
        parsed_events | 'WriteRawEvents' >> beam.io.WriteToBigQuery(
            table=BIGQUERY_RAW_TABLE, schema=raw_schema_string,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

        # Branch 2: Player Score Updates
        (
            parsed_events
            | 'CalculatePlayerScores' >> beam.ParDo(CalculatePlayerScores())
            | 'WindowPlayerScores' >> beam.WindowInto(FixedWindows(60))
            | 'GroupPlayerScores' >> beam.GroupByKey()
            | 'UpdatePlayerTotals' >> beam.ParDo(UpdateScoreWithState())
            | 'FormatPlayerUpdates' >> beam.ParDo(FormatScoreUpdate(), 'player_id', 'total_score')
            | 'WritePlayerScoreUpdates' >> beam.io.WriteToBigQuery(
                table=BIGQUERY_PLAYER_SCORE_UPDATES,
                schema=player_score_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

        # Branch 3: Team Score Updates
        (
            parsed_events
            | 'CalculateTeamScores' >> beam.ParDo(CalculateTeamScores())
            | 'WindowTeamScores' >> beam.WindowInto(FixedWindows(60))
            | 'GroupTeamScores' >> beam.GroupByKey()
            | 'UpdateTeamTotals' >> beam.ParDo(UpdateScoreWithState())
            | 'FormatTeamUpdates' >> beam.ParDo(FormatScoreUpdate(), 'team_id', 'total_wins')
            | 'WriteTeamScoreUpdates' >> beam.io.WriteToBigQuery(
                table=BIGQUERY_TEAM_SCORE_UPDATES,
                schema=team_score_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == '__main__':
    run()
