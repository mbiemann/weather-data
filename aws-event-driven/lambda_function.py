import awswrangler as wr
import boto3
import json
import os

expected_columns = [
    'ForecastSiteCode','ObservationTime','ObservationDate','WindDirection','WindSpeed','WindGust','Visibility',
    'ScreenTemperature','Pressure','SignificantWeatherCode','SiteName','Latitude','Longitude','Region','Country'
]

def replace_incoming(source,target):
    return source.replace('event-incoming','event-'+target)

def move_file(bucket,key,target):
    source_prefix = '/'.join(key.split('/')[:-1])
    target_prefix = replace_incoming(source_prefix,target)
    wr.s3.copy_objects(
        paths=[f's3://{bucket}/{key}'],
        source_path=f's3://{bucket}/{source_prefix}',
        target_path=f's3://{bucket}/{target_prefix}'
    )
    wr.s3.delete_objects([f's3://{bucket}/{key}'])

def lambda_handler(event, context):

    env = os.environ['ENVIRONMENT']
    
    # create database if need
    database = f'weather_{env}'
    wr.catalog.create_database(database,exist_ok=True)

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        try:

            # check file extension
            if key[-4:] != '.csv':
                raise Exception('Invalid file extension. Expected a CSV file.')
            
            # read file
            df = wr.s3.read_csv(f's3://{bucket}/{key}')

            # check input columns
            if df.columns.to_list() != expected_columns:
                raise Exception('Invalid columns.')

            # write parquet
            wr.s3.to_parquet(
                df=df,
                path=f's3://{bucket}/refined/',
                dataset=True,
                mode='append',
                database=database,
                table='observation'
            )

            # move file to processed
            move_file(bucket,key,'processed')

        except Exception as e:
            print('ERROR: '+str(e))

            # move file to error
            move_file(bucket,key,'error')

            # create log file
            boto3.client('s3').put_object(
                Bucket=bucket,
                Key=replace_incoming(key,'error')+'.log',
                Body=str(e).encode()
            )
