import os
import sys
import logging
import boto3
import yaml
import json
from botocore.exceptions import ClientError
import base64
import ast
from dataclasses import asdict

LOGGER = logging.getLogger(__name__)


def load_env():
    """Load enviroment variables."""
    try:
        return {
            "app": {
                "LOGGING_LEVEL": os.environ["LOGGING_LEVEL"],
                "APP_ENV": os.environ['APP_ENV']
            },
            "secretsmanager":{
                "REDSHIFT_CREDENTIALS": ast.literal_eval(_get_secret(os.environ['REDSHIFT_CREDENTIALS'])['SecretString'])
            }
        }
    except KeyError as error:
        LOGGER.exception("Enviroment variable %s is required.", error)
        sys.exit(1)

def _get_secret(secret_name):
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    
    return get_secret_value_response