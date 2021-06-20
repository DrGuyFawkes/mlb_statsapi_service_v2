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
                "APP_ENV": os.environ['APP_ENV']
            },
            "s3":{
                "BUCKET" : os.environ['BUCKET']
                }
        }
    except KeyError as error:
        LOGGER.exception("Enviroment variable %s is required.", error)
        sys.exit(1)