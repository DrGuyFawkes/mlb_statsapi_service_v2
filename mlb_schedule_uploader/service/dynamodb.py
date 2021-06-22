import boto3
from boto3.dynamodb.conditions import Key
import json

def get_update_params(body):
    """Given a dictionary we generate an update expression and a dict of values
    to update a dynamodb table.

    Params:
        body (dict): Parameters to use for formatting.

    Returns:
        update expression, dict of values.
    """
    update_expression = ["set "]
    update_values = dict()

    for key, val in body.items():
        update_expression.append(f" {key} = :{key},")
        update_values[f":{key}"] = val

    return "".join(update_expression)[:-1], update_values

def update_metadata_dynamodb(dynamodb_table, record):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table)

    gamePk = str(record['gamePk'])
    del record['gamePk']

    a, v = get_update_params(record)
    
    response = table.update_item(
        Key={"gamePk": gamePk},
        UpdateExpression=a,
        ExpressionAttributeValues=dict(v)
        )

    return print('updated successfully ', dynamodb_table)