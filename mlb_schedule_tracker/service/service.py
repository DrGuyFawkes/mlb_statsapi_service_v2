import logging
import json
import datetime
from service import mlb_statsapi_client, sqs
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
        start_date_utc = datetime.datetime.utcnow() - datetime.timedelta(days = 1)
        end_date_utc = datetime.datetime.utcnow() + datetime.timedelta(days = 1)
        start = datetime.datetime.strptime(start_date_utc.strftime("%m/%d/%Y") ,"%m/%d/%Y") if not event.get('start_date')  else datetime.datetime.strptime(event['start_date'], "%m/%d/%Y")
        end = datetime.datetime.strptime(end_date_utc.strftime("%m/%d/%Y") ,"%m/%d/%Y") if not event.get('end_date')  else datetime.datetime.strptime(event['end_date'], "%m/%d/%Y")

        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]

        for date in date_generated:

            current_date = date.strftime("%m/%d/%Y")

            # Get schedule
            # list up all levels
            # MLB = '1'
            # AAA = '11'
            # AA = '12'
            # High A = '13'
            # Low A = '14'
            # ShortSeason A = '15'
            # Rookie Advanced = '5442'
            # Rookie = '16':

            #sport_ids = ['1','11','12','13','14','15','5442','16']
            sport_ids = ['1']

            for sport_id in sport_ids:
                games = mlb_statsapi_client.get_schedule_statsapi(sport_id, current_date)

                if len(games) >= 1:
                    # each queue has a deley seconds. It provent from all 
                    deley_seconds = 0
                    deley_seconds_gap = int(900/len(games))

                    for game in games:

                        queue_url = environment["sqs"]["SCHEDULE_QUEUE"]

                        message_body = json.dumps({
                                                    "game_pk" : str(game['gamePk']),
                                                    "sport_id" : sport_id
                                                    })

                        publish_queue = sqs.publish_sqs(queue_url,message_body,deley_seconds)
                        
                        deley_seconds += deley_seconds_gap

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