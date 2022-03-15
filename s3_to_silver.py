import json
import boto3
import pandas as pd
import os
import redshift_connector

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from urllib.parse import quote_plus


class RedShiftEngineAlchemy:
    def __init__(self, host, user, passwd):
        self.host = f'{host}.redshift-serverless.amazonaws.com'
        self.port = 5439
        self.username = user
        self.password = passwd

    def get_url(self, dbname):
        uri_rs = URL.create(drivername='redshift+redshift_connector', # indicate redshift_connector driver and dialect will be used
                         host=self.host, # Amazon Redshift host
                         port=5439, # Amazon Redshift port
                         database=dbname, # Amazon Redshift database
                         username=self.username,
                         password=self.password
                         )
                         
        #uri_rs = f"redshift:///?User={self.username}&;Password={self.password }&Database={dbname}&Server={self.host}.amazonaws.com&Port={self.port}"
        #uri_rs = f'redshift://{self.username}:{self.password}@{self.host}:{self.port}/{dbname}'
        print(uri_rs)
        #print(url)
        return uri_rs
        
    #red_engine = create_engine(
    #    'redshift+psycopg2://username:password@your.redshift.host.123456abcdef.us-east-1.redshift.amazonaws.com:port/database')

    def get_engine(self, dbname):
        url = self.get_url(dbname)
        conn_params = {
                        "iam": True, # must be enabled when authenticating via IAM
                        "credentials_provider": "OktaCredentialsProvider",
                        "idp_host": "sunbit.okta.com",
                        
                        "app_name": "amazon_aws_redshift",
                        "region": "us-east-1",
                        
                        "ssl_insecure": True, # ensures certificate verification occurs for idp_host
                       }
        engine = create_engine(url, connect_args=conn_params)
        #config = {
        #    "sqlalchemy.url": url,
        #    "sqlalchemy.echo": True,
        #    "sqlalchemy.server_side_cursors": True,
        #}
        #engine = engine_from_config(config)
        return engine



class RedShiftConn:
    def __init__(self, host, user, passwd, dbname):
        self.host = f'{host}.redshift-serverless.amazonaws.com'
        self.port = 5439
        self.username = user
        self.password = passwd
        self.dbname = dbname
        self.conn = self.create_connection()

    def create_connection(self):
        conn = redshift_connector.connect(
            host=self.host,
            database=self.dbname,
            user=self.username,
            password=self.password
        )
        return conn

    def get_connection(self):
        return self.conn


if __name__ == "__main__":
    dbname = 'silver'
    dbname = "sample_data_dev"
    dbname = "dev"
    hostname = '612580448941.us-east-1'
    #db_conn = RedShiftConn(hostname, 'tamar.domany', 'Doman11i!i', dbname)

    print(quote_plus('Doman11i!i'))
    db_conn = RedShiftEngineAlchemy(hostname,'tamar.domany',quote_plus('Doman11i!i'))
    db_engine = db_conn.get_engine(dbname)
    
    '''
    cursor = conn.get_connection().cursor()
    cursor.execute('select * from "sample_data_dev"."tickit"."users"')
    fa = cursor.fetchall()
    print(fa)
    '''


    s3_obj =boto3.client('s3')
    s3_clientobj = s3_obj.get_object(Bucket='datalake-test-tamar-bucket', Key='colors.json')
    s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
    '''
    print("printing s3_clientdata")
    print(s3_clientdata)
    print(type(s3_clientdata))
    '''
    s3clientlist=json.loads(s3_clientdata)
    df = pd.json_normalize(s3clientlist['colors'])
    print(df)
    df.to_sql('colors', db_engine, index=False, if_exists='replace')

    print()

