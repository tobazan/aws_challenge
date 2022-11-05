import pandas as pd
from pandas import to_datetime
import psycopg2
import psycopg2.extras as extras
import boto3
import urllib.parse
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

def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
        print("Dataframe successfully inserted")
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

def get_json(event):
    s3_client = boto3.client('s3')
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response

    except Exception as e:
        print('Error getting object {} from bucket {}'.format(key, bucket), e)
        raise e

def flatten_json(json_file):
    try:
        #df from json data
        df = pd.DataFrame.from_records(json_file)
        
        #explode hits
        df_exploded = df.explode('hit', ignore_index=True)
        df_exploded['date'] = pd.to_datetime(df_exploded['date'], format='%Y%m%d')
        
        #group by users sessions
        hit_counts_df = pd.DataFrame(df_exploded.groupby(["userId","visitId","date","device","city"])['hit'].count().sort_index()).reset_index()
        hit_counts_df.columns = ["userId","visitId","date","device","city","hit_count"]

        return hit_counts_df

    except Exception as e:
        print('Error trying to flatten json data', e)
        raise e

####################  MAIN  ####################
def lambda_handler(event, context):
    
    json_file = get_json(event)
    hit_counts_df = flatten_json(json_file)
    
    try:
        conn = rds_connection()
    except Exception as e:
        print('Error connecting to RDS DB', e)
        raise e

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monks.hits (
                    userId decimal NOT NULL,
                    visitId integer NOT NULL,
                    date date NOT NULL,	
                    device VARCHAR (255),	
                    city VARCHAR (255),	
                    hit_count integer NOT NULL
                    );
            """)

    except Exception as e:
        print('Error creating hits table in RDS DB', e)
        raise e

    try:
        execute_values(conn, hit_counts_df, 'monks.hits')

    except Exception as e:
        print('Error trying to write in RDS DB hits table', e)
        raise e

    conn.close()
