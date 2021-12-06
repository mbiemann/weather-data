import boto3
import json
import os

def lambda_handler(event, context):

    bucket = os.environ['BUCKET']
    path = 'batch-incoming/'

    resp = boto3.client('s3').list_objects_v2(
        Bucket=bucket,
        Prefix=path
    )

    if resp['KeyCount'] == 0:
        # Cancel execution if no files

        boto3.client('stepfunctions').stop_execution(
            executionArn=event['ExecutionId'],
            error='There are no files to process',
            cause=f'No files were found in the s3://{bucket}/{path}.'
        )

    else:
        # If files, return bucket and keys

        keys = list()
        for content in resp['Contents']:
            keys.append(content['Key'])

        return json.dumps({
            "Bucket": bucket,
            "Keys": keys
        })
