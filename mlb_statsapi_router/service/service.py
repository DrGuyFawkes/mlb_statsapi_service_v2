import logging
from service import sqs, dynamodb_json
import sys
import json

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
        for record in event['Records']:

            if record['eventName'] in ['INSERT','MODIFY']:
                convert_json = dynamodb_json.unmarshal_dynamodb_json(record['dynamodb']['NewImage']) 

                # playbyplay
                if 'gamePk' in convert_json.keys():
                    playbyplay_queue_url = environment['sqs']['PLAYBYPLAY_QUEUE']         
                    playbyplay_message = json.dumps({
                                                    'gamePk':convert_json['gamePk'],
                                                    'type':'playbyplay'
                                                    })
                    response = sqs.publish_sqs(playbyplay_queue_url, playbyplay_message)

                # venue
                if 'venue' in convert_json.keys():
                    venue_queue_url = environment['sqs']['VENUE_QUEUE']    
                    venue_message = json.dumps({
                                                'link':convert_json['venue'],
                                                'type':'venue'
                                                })
                # teams
                if 'teams' in convert_json.keys():
                    teams_queue_url = environment['sqs']['TEAMS_QUEUE']
                    for key in convert_json['teams'].keys():
                        if key in ['away', 'home']:
                            link = convert_json['teams'][key]['team']['link']
                            team_message = json.dumps({
                                                      'link': link,
                                                      'type':'teams'
                                                      })
                            response = sqs.publish_sqs(teams_queue_url, team_message)

                # provided venue endpoint has name of venue only. For now, this service is not using venue endpoint. The venue will pull from livefeed endpoint. 
                # livefeed
                if 'link' in convert_json.keys():
                    livefeed_queue_url = environment['sqs']['LIVEFEED_QUEUE']   
                    livefeed_message = json.dumps({
                                                'link':convert_json['link'],
                                                'type':'livefeed'
                                                })
                    response = sqs.publish_sqs(livefeed_queue_url, livefeed_message)                

            elif record['eventName'] == 'REMOVE':
                pass
                    
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