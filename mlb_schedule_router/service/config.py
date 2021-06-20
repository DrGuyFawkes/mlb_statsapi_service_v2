import os
import sys
import logging
import boto3
import yaml
import json
from botocore.exceptions import ClientError

LOGGER = logging.getLogger(__name__)


def load_env():
    """Load enviroment variables."""
    try:
        return {
            "app": {
                "LOGGING_LEVEL": os.environ["LOGGING_LEVEL"],
                "APP_ENV": os.environ['APP_ENV']},
            "sqs":{                
                   "PLAYBYPLAY_QUEUE": os.environ['PLAYBYPLAY_QUEUE'],
                   "LIVEFEED_QUEUE": os.environ['LIVEFEED_QUEUE'],
                   "VENUE_QUEUE": os.environ['VENUE_QUEUE'],
                   "TEAMS_QUEUE": os.environ['TEAMS_QUEUE']
                   }
        }
    except KeyError as error:
        LOGGER.exception("Enviroment variable %s is required.", error)
        sys.exit(1)