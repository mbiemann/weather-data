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

from pyspark.sql.functions import col, lit

#===============================================================================

def answer_questions(table, df):
    print('Answering questions...')

    new_temp = df.select(col('ScreenTemperature')).max().collect()[0]

    print('NEW TEMP')
    print(new_temp)

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

    # Move files to in-progress
    move_files(bucket, keys, 'batch-incoming/', f'batch-in-progress/{load_dt}_')

    try:
        df = None

        # Read files
        for key in keys:
            key = key.replace('batch-incoming/', f'batch-in-progress/{load_dt}_')

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
        move_files(bucket, keys, f'batch-in-progress/{load_dt}_', f'batch-processed/{load_dt}_')

        # Answer Questions
        answer_questions(question_table, df)

        # Waiting Uncache
        df.unpersist(blocking=True)

    except Exception as e:
        print(f'ERROR: {e}')

        # Move files to error
        move_files(bucket, keys, f'batch-in-progress/{load_dt}_', f'batch-error/{load_dt}_')

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