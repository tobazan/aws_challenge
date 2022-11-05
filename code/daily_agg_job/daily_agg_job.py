import pandas as pd
from pandas import to_datetime
import psycopg2
import psycopg2.extras as extras
import boto3
import json

sm = boto3.client('secretsmanager')

secret = sm.get_secret_value(SecretId='postgres-credentials')
secrets_dict = json.loads(secret['SecretString'])

def rds_connection():
    conn = psycopg2.connect(
        host=secrets_dict['host'],
        database='postgres',
        user=secrets_dict['username'],
        password=secrets_dict['password'])
    return conn

####################  MAIN  ####################
def lambda_handler(event, context):
    
    try:
        conn = rds_connection()
        conn.autocommit = True
    except Exception as e:
        print('Error connecting to RDS DB', e)
        raise e

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monks.daily_hits_agg (
                    date date NOT NULL,
                    hits_total_count integer NOT NULL,
                    users_total_count integer NOT NULL
                    );
            """)
    except Exception as e:
        print('Error cerating agg table', e)
        raise e

    try:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE monks.daily_hits_agg;")
    except Exception as e:
        print('Error truncating agg table', e)
        raise e

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO monks.daily_hits_agg (date, hits_total_count, users_total_count)
                SELECT date, 
                    SUM(hit_count) AS hits_total_count, 
                    COUNT(distinct userId) AS users_total_count
                FROM monks.hits
                GROUP BY date;
            """)
    except Exception as e:
        print('Error making daily agg calculations', e)
        raise e

    conn.close()