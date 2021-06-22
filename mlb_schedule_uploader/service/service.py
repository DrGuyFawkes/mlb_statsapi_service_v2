import logging
import json
import datetime
from service import mlb_statsapi_client, dynamodb
import sys

LOGGER = logging.getLogger(__name__)

def main(event, environment):
    """
    Entrypoint to service.
    
    :param event: AWS Event Trigger
    :type event: Dict[str, Any]
    :param environment: Dictionary containing env variables.
    :type environment: Dict[str, Any]
    """
    
    LOGGER.info(event)

    # error message template
    execution_event = {
        'status': '',
        'description': '',
        'metadata': {}
    }

    try:
        for record in event["Records"]:
            # message_body: game_pk and sport_id
            message_body = json.loads(event["Records"][0]['body'])
            
            games = mlb_statsapi_client.get_schedule_statsapi(message_body)

            if len(games) >= 1:
                schedule_dynamodb = environment['dynamodb']['SCHEDULE_DYNAMODB_TABLE']

                for game in games:
                    response = dynamodb.update_metadata_dynamodb(schedule_dynamodb, game)


    except KeyError as e:
        error = f"Missing required field {e}."
        LOGGER.error(error)
        execution_event.update({"status": "Error", "description": error, "metadata": event})
        sys.exit(1)
    except Exception as e:
        LOGGER.error(e)
        execution_event.update({"status": "Error", "description": f"{str(e)}", "metadata": event})
        sys.exit(1)
    finally:
        LOGGER.info(execution_event)
    
    return execution_event