import logging
from dataclasses import asdict
import json
from service import mlb_statsapi_client, s3
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

    # access to aws client
    #s3_client = awsclients.aws_client('s3')
    #sqs = awsclients.aws_client('sqs')

     # error message template
    execution_event = {
        'status': '',
        'description': '',
        'metadata': {}
    }

    try:
        for record in event["Records"]:

            message_body = json.loads(event["Records"][0]['body'])
            game_pk = message_body['gamePk']
            request_type = message_body['type'].lower()
    
            if request_type == 'playbyplay':
                output = mlb_statsapi_client.get_playbyplay_statsapi(game_pk)
            
            elif request_type == 'livefeed':
                pass

            elif request_type == 'teams':
                pass

            elif request_type == 'players':
                pass

            elif request_type == 'venue':
                pass


            if len(output) > 0:
                bucket = environment['s3']['BUCKET']
                response = s3.put_csv_to_s3(bucket, game_pk, request_type, output)

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