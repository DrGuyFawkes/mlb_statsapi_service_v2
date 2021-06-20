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
        start_date_utc = datetime.datetime.utcnow() - datetime.timedelta(days = 5)
        end_date_utc = datetime.datetime.utcnow() + datetime.timedelta(days = 5)
        start = datetime.datetime.strptime(start_date_utc.strftime("%m/%d/%Y") ,"%m/%d/%Y") if not event.get('start_date')  else datetime.datetime.strptime(event['start_date'], "%m/%d/%Y")
        end = datetime.datetime.strptime(end_date_utc.strftime("%m/%d/%Y") ,"%m/%d/%Y") if not event.get('end_date')  else datetime.datetime.strptime(event['end_date'], "%m/%d/%Y")

        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]

        for date in date_generated:
            current_date = date.strftime("%m/%d/%Y")
            games = mlb_statsapi_client.get_schedule_statsapi(current_date)

            if len(games) >= 1:
                # only read game that is not in trackman metadata table
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