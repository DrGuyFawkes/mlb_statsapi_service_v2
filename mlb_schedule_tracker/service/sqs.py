import boto3
import json

def publish_sqs(queue_url,message,delay_seconds):

    sqs = boto3.client('sqs')
    # publish message to sqs per every game ingested
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=(
            str(message)
        ),
        DelaySeconds = delay_seconds               
    )