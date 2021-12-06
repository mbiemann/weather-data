import awswrangler as wr
import datetime
import boto3
import os

#===============================================================================

_expected_columns = [
    'ForecastSiteCode','ObservationTime','ObservationDate','WindDirection','WindSpeed','WindGust','Visibility',
    'ScreenTemperature','Pressure','SignificantWeatherCode','SiteName','Latitude','Longitude','Region','Country'
]

#===============================================================================

def _set_answer(answer,question):

    boto3.client('dynamodb').put_item(
        TableName=_table,
        Item={
            'question':{'S':question},
            'answer':{'S':answer}
        }
    )

#===============================================================================

def _get_answer(question,default):
    answer = default

    try:
        answer = boto3.client('dynamodb').get_item(
            TableName=_table,
            Key={'question':{'S':question}},
            ConsistentRead=True
        )['Item']['answer']['S']
    except Exception as e:
        print(f'Get Item ERROR: {e}')

    return answer

#===============================================================================

def _answer_questions(df):
    print('Answering questions...')

    new_temp = str(df['screen_temperature'].max())

    old_temp = _get_answer( 'What was the temperature on that day?', 0 )

    if ( float(new_temp) > float(old_temp) ):

        new_date = str(df[ df['screen_temperature'] == df['screen_temperature'].max() ]['observation_date'].to_numpy()[0])[:10]
        new_regn = str(df[ df['screen_temperature'] == df['screen_temperature'].max() ]['region'].to_numpy()[0])

        _set_answer( new_temp, 'What was the temperature on that day?' )
        _set_answer( new_date, 'Which date was the hottest day?' )
        _set_answer( new_regn, 'In which region was the hottest day?' )

        print('Answered questions!')

    else:

        print('None questions updated!')

#===============================================================================

def _move_file(bucket,key,source,target,load_dt=''):

    # source params
    source_prefix = '/'.join(key.split('/')[:-1])
    source_filename = key.split('/')[-1]

    # target params
    target_prefix = source_prefix.replace(source,target)
    target_filename = load_dt + '_' + source_filename if load_dt != '' else source_filename
    target_key = target_prefix+'/'+target_filename

    # copy
    boto3.client('s3').copy_object(
        CopySource={
            "Bucket": bucket,
            "Key": key
        },
        Bucket=bucket,
        Key=target_key
    )

    # delete
    boto3.client('s3').delete_object(
        Bucket=bucket,
        Key=key
    )

    # print
    print(f'Moved s3://{bucket}/{key} to s3://{bucket}/{target_key}!')

    # return new key
    return target_key

#===============================================================================

def process(database, question_table, bucket, key, size):
    print(f'Processing s3://{bucket}/{key}...')

    try:
        actual = 'incoming'

        # check file extension
        if key[-4:] != '.csv':
            raise Exception('Invalid file extension. Expected a CSV file.')

        # check file size
        if size >= 11000000:
            raise Exception(f'File size {size} must be less than {11000000} bytes. Use Batch Process for this file.')

        # get original filename
        filename = key.split('/')[-1]
        load_dt = datetime.datetime.utcnow().isoformat()

        # move file to in-progress
        key = _move_file(bucket,key,actual,'in-progress',load_dt)

        # read file
        print('CSV reading...')
        df = wr.s3.read_csv(f's3://{bucket}/{key}')
        print('CSV read!')

        # check input columns
        if df.columns.to_list() != _expected_columns:
            raise Exception('Invalid columns.')

        # new columns
        df['filename'] = filename
        df['load_type'] = 'EVENT-DRIVEN'
        df['load_dt'] = load_dt

        # write parquet
        print('Parquet writing...')
        wr.s3.to_parquet(
            df=df,
            path=f's3://{bucket}/refined/',
            dataset=True,
            mode='append',
            database=_database,
            table='observation'
        )
        print('Parquet written!')

        # move file to processed
        _move_file(bucket,key,'in-progress','processed')

    except Exception as e:
        print('ERROR: '+str(e))

        # move file to error
        key = _move_file(bucket,key,'in-progress','error')

        # create log file
        print('Logging error...')
        boto3.client('s3').put_object(
            Bucket=bucket,
            Key=key+'.log',
            Body=str(e).encode()
        )
        print('Logged error!')

        # if error do not continue
        return

    # Answer Questions
    _answer_questions(df)

#===============================================================================

def lambda_handler(event, context):

    # Get Variables defined on CloudFormation
    database = os.environ['DATABASE']
    question_table = os.environ['QUESTION_TABLE']

    # Create database if not exists
    wr.catalog.create_database(database, exist_ok=True)

    # Loop on records
    for record in event['Records']:

        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        size = record['s3']['object']['size']

        # Process file
        process(database, question_table, bucket, key, size)

#===============================================================================