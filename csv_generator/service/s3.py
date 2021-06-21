import boto3
import json
import io
from io import StringIO
from datetime import datetime
import csv
import os

def put_csv_to_s3(bucket_name, identifier, reqeust_type, records):

    class Pipe:
        value = ""
        def write(self, text):
            self.value = self.value + text
   
    s3 = boto3.client('s3')

    now = datetime.utcnow()

    pipe = Pipe()

    field_names = []
    for item in records:

        # add ingestion timestamp
        item['ingestion_timestamp_utc'] = now.strftime("%Y-%m-%d %H:%M:%S")  

        for dict_key in item.keys():
            if dict_key not in field_names:
                field_names.append(dict_key)

    writer = csv.DictWriter(pipe, fieldnames=field_names, delimiter='|')

    writer.writeheader()
    for entry in records:

        row_data = {key: value for key, value in entry.items() if key in field_names}
        
        writer.writerow(row_data)

   
    current_timestamp = now.strftime("%Y%m%d%H%M%S")
    current_year = str(now.year)
    current_month = str("{:02d}".format(now.month))
    current_day = str("{:02d}".format(now.day))
    current_hour = str("{:02d}".format(now.hour))

    keyPath = f'data/rds_ingestion_data/{reqeust_type}/year={current_year}/month={current_month}/day={current_day}/hour={current_hour}/{reqeust_type}_{identifier}_{current_timestamp}.csv'


    response = s3.put_object(Bucket=bucket_name, Body= pipe.value, Key=keyPath)

    return print("successfully ingest csv file to ",bucket_name,'/',keyPath)