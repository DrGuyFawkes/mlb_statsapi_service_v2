import logging
from dataclasses import asdict
import json
from service import mlb_statsapi_client, s3, sqs
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
            request_type = message_body['type'].lower()
    
            if request_type == 'playbyplay':
                identifier = message_body['gamePk']
                output = mlb_statsapi_client.get_playbyplay_statsapi(identifier)
            
            
            elif request_type == 'livefeed': # livefeed + venue
                link = message_body['link']
                identifier = link.split('/')[-3]
                livefeed = mlb_statsapi_client.get_livefeed_statsapi(link)
                output = livefeed[0]
                players = livefeed[1]

                # send player queue to get a player data from the player endpoint
                players_queue_url = environment['sqs']['PLAYERS_QUEUE']
                player_message = json.dumps({
                                            'link':players,
                                            'type':'players',
                                            'gamePk':identifier
                                            })
                response = sqs.publish_sqs(players_queue_url, player_message)

            elif request_type == 'teams':
                link = message_body['link']
                identifier = link.split('/')[-1]
                output = mlb_statsapi_client.get_teams_statsapi(link)

            elif request_type == 'players':
                player_links = message_body['link']
                identifier = message_body['gamePk']
                
                output = []
                for link in player_links:
                    player_data = mlb_statsapi_client.get_players_statsapi(link)
                    output.extend(player_data)

            # provided venue endpoint has name of venue only. For now, this service is not using venue endpoint. The venue will pull from livefeed endpoint.
            elif request_type == 'venue':
                pass


            if 'output' in vars() and len(output) > 0:
                bucket = environment['s3']['BUCKET']
                response = s3.put_csv_to_s3(bucket, identifier, request_type, output)

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