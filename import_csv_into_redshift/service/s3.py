import os
import boto3
import pandas as pd
import sys
from io import StringIO
from dataclasses import asdict

def read_csv_from_s3(bucket_name, object_key):

    client = boto3.client('s3')

    csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')

    dataframe = pd.read_csv(StringIO(csv_string), sep="|")

    return dataframe