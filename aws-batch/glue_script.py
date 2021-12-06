#===============================================================================
#=== Creation: 2021-12-04 by Marcell Biemann
#===
#=== To analyse this code, follow the methods flow:
#===   1. main
#===   2. process
#===        move_files
#===   3. answer_questions
#===
#===============================================================================

import boto3
import datetime
import json
import sys

# Glue
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from pyspark.sql.functions import col, lit, max

#===============================================================================

def set_answer(table, question, answer):

    boto3.client('dynamodb').put_item(
        TableName=table,
        Item={
            'question':{'S':question},
            'answer':{'S':answer}
        }
    )

#===============================================================================

def get_answer(table, question, default):
    answer = default

    try:
        answer = boto3.client('dynamodb').get_item(
            TableName=table,
            Key={'question':{'S':question}},
            ConsistentRead=True
        )['Item']['answer']['S']
    except Exception as e:
        print(f'Get Item ERROR: {e}')

    return answer

#===============================================================================

def answer_questions(table, df):
    print('Answering questions...')

    # get new and old high temperature
    new_temp = df.select(col('ScreenTemperature').cast('float')).groupBy().max().first()[0]
    old_temp = float(get_answer( table, 'What was the temperature on that day?', 0 ))

    # if new is highest than old
    if new_temp > old_temp:

        # filter record with highest temperature
        df = df.filter(col('ScreenTemperature').cast('float') == new_temp)

        # get date and region
        new_date = df.select(col('ObservationDate')).first()[0][:10]
        new_regn = df.select(col('Region')).first()[0]

        # update table
        set_answer( table, 'What was the temperature on that day?', "{:.2f}".format(new_temp) )
        set_answer( table, 'Which date was the hottest day?', new_date )
        set_answer( table, 'In which region was the hottest day?', new_regn )

        print('Answered questions!')

    else:

        print('None questions updated!')


#===============================================================================

def move_files(bucket, keys, source, target):

    for key in keys:
        source_key = key.replace('batch-incoming/',source)
        target_key = key.replace('batch-incoming/',target)

        # copy
        boto3.client('s3').copy_object(
            CopySource={
                "Bucket": bucket,
                "Key": source_key
            },
            Bucket=bucket,
            Key=target_key
        )

        # delete
        boto3.client('s3').delete_object(
            Bucket=bucket,
            Key=source_key
        )

        # print
        print(f'Moved s3://{bucket}/{source_key} to s3://{bucket}/{target_key}!')

#===============================================================================

def process(spark, database, question_table, bucket, keys):

    # Create database if not exists
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {database}')

    # Load Datetime Isoformat
    load_dt = datetime.datetime.utcnow().isoformat()

    actual = 'batch-incoming/'

    try:
        df = None

        # Move files to in-progress
        move_files(bucket, keys, actual, f'batch-in-progress/{load_dt}_')
        actual = f'batch-in-progress/{load_dt}_'

        # Read files
        for key in keys:
            key = key.replace('batch-incoming/', actual)

            # Read CSV
            df_read = spark.read \
                .option('header','true') \
                .csv(f's3://{bucket}/{key}')

            # Add Filename Column
            df_read = df_read.withColumn('filename',lit(key.split('/')[-1]))

            # Union All
            df = df_read if df == None else df.unionAll(df_read)

        # Add Load Type and Datetime Columns
        df = df.withColumn('load_type',lit('BATCH'))
        df = df.withColumn('load_dt',lit(load_dt))

        # Caching
        df.persist()

        # Write
        df.write \
            .format('parquet') \
            .option('path',f's3://{bucket}/refined/') \
            .mode('append') \
            .saveAsTable(f'{database}.observation')

        # Move files to processed
        move_files(bucket, keys, actual, f'batch-processed/{load_dt}_')
        actual = f'batch-processed/{load_dt}_'

        # Answer Questions
        answer_questions(question_table, df)

        # Waiting Uncache
        df.unpersist(blocking=True)

    except Exception as e:
        print(f'ERROR: {e}')

        # Move files to error
        move_files(bucket, keys, actual, f'batch-error/{load_dt}_')

        # Log
        for key in keys:
            log_key = key.replace('batch-incoming/',f'batch-error/{load_dt}_')+'.log'
            boto3.client('s3').put_object(
                Bucket=bucket,
                Key=log_key,
                Body=str(e).encode()
            )
            print(f'Log written: {log_key}!')

#===============================================================================

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    spark = glueContext.spark_session

    # Jobs Arguments
    args = getResolvedOptions(sys.argv,[
        'JOB_NAME',
        'DATABASE',
        'QUESTION_TABLE',
        'CONTENT',
    ])
    job_name = args['JOB_NAME']
    database = args['DATABASE']
    question_table = args['QUESTION_TABLE']
    content = json.loads(args['CONTENT'])
    bucket = content['Bucket']
    keys = content['Keys']

    # Start
    print('START')
    job.init(job_name,args)

    # Process
    process(spark, database, question_table, bucket, keys)

    # End
    print('END')
    job.commit()

#===============================================================================

if __name__ == '__main__':
    main()

#===============================================================================