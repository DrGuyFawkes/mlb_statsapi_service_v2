import boto3
import json

''' example of message(String)
message= 'Successfully ingested trackman csv file into s3: ' + file  
'''

# example: athena_query(athena,s3,params)
def publish_sqs(queue_url,message):

    sqs = boto3.client('sqs')

    # publish message to sqs per every game ingested
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=(
            str(message)
        )              
    )