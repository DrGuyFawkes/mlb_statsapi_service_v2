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
            "dynamodb":{
                 "SCHEDULE_DYNAMODB_TABLE":os.environ['SCHEDULE_DYNAMODB_TABLE']
            }
        }
    except KeyError as error:
        LOGGER.exception("Enviroment variable %s is required.", error)
        sys.exit(1)


def _retrieve_parameters(path):
    """
    Retrieve application secrets from AWS parameter store.
    
    :param name: The name of the secret to retrieve.
    :type name: str
    """
    try:
        client = boto3.client('ssm')
        response = client.get_parameters_by_path(Path=path, WithDecryption=True)
        params = [json.loads(parameter['Value']) for parameter in response['Parameters']]
        while response.get('NextToken'):
            response = client.get_parameters_by_path(Path=path, WithDecryption=True, NextToken=response['NextToken'])
            params += [json.loads(parameter['Value']) for parameter in response['Parameters']]
    except ClientError as e:
        LOGGER.exception("Failed to retieve secrets from AWS parameter store.", extra=e.response)
        sys.exit(1)
    else:
        return params