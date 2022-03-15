import json
import boto3
import pandas as pd
import re

from sqlalchemy_redshift.dialect import SUPER
from redshift_sa_conn import RedShiftEngineAlchemy, AwsRedshiftConn
from redshift_conn import RedShiftConn


def read_s3_json(bucket, json_file_name):
    s3_obj = boto3.client('s3')
    s3_clientobj = s3_obj.get_object(Bucket=bucket, Key=json_file_name)
    s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
    file_dict = json.loads(s3_clientdata)
    return file_dict
    

def get_insert_query_from_df(df, dest_table):
    insert = f'INSERT INTO "{dest_table}" '

    columns_string = str(list(df.columns)) [1:-1]
    columns_string = re.sub(r' ', '\n        ', columns_string)
    columns_string = re.sub(r"\'", "", columns_string)

    records = list(df.to_records(index=False))
    values_string = re.sub(r'nan', 'null', str(records))
    #values_string = re.sub(r"\'", "\"", values_string)
    values_string = re.sub(r"'{", "JSON_PARSE(\'{", values_string)
    values_string = re.sub(r"}'", "}\')", values_string)
    values_string = values_string[1:-1].replace('), ','),\n')
    
    return f"{insert} ({columns_string}) \n VALUES\n {values_string};"


def colors_to_sql(db_engine, schema, color_df):
    table_name = "colors"
    super_fields = ['code', 'code_rgba']
    drop_query = f'DROP TABLE IF EXISTS "{schema}"."{table_name}";'
    
    dtype_dict = {c: SUPER for c in super_fields}
    create_query = pd.io.sql.get_schema(color_df, table_name, keys=['color'], con=db_engine, dtype=dtype_dict)

    db_engine.execute(drop_query)
    db_engine.execute(create_query)

    for col in super_fields:
        color_df.loc[:,col] = color_df[col].apply(lambda x: json.dumps(x))
    
    non_super_cols = [c for c in df.columns if c not in super_fields]
    insert_query = get_insert_query_from_df(color_df, table_name)
    
    print(insert_query)
    db_engine.execute(insert_query)


if __name__ == "__main__":
    dbname = 'dev'
    schema = 'public'
    db_conn = AwsRedshiftConn()
    db_engine = db_conn.get_engine(dbname)
    
    '''
    db_conn = RedShiftConn(hostname, 'awsuser' ,'Awsuser1', dbname)
    cursor = db_conn.get_connection().cursor()
    cursor.execute('SELECT * FROM "dev"."public"."users" limit 10')
    fa = cursor.fetchall()
    print(fa)
    '''
    
    s3_color_dict = read_s3_json('datalake-test-tamar-bucket', 'colors.json')
    
    df0 = pd.DataFrame(s3_color_dict['colors'])
    df = pd.concat([df0, pd.json_normalize(df0['code'])], axis=1)
    df.rename(columns={'rgba': 'code_rgba', 'hex': 'code_hex'}, inplace=True)
    #df.loc[:, 'code_hex'] = df['code_hex'].astype(str)
    df.loc[:, 'code_rgba_str'] = df['code_rgba'].astype(str)
    print(df)
    #df.to_sql('colors', db_engine, index=False, if_exists='replace')
    colors_to_sql(db_engine, schema, df)
    
    
    s3_user_dct = read_s3_json('datalake-test-tamar-bucket', 'test_data.json')
    df2 = pd.DataFrame(s3_user_dct)
    print(df2)
    #df2.to_sql('users_test', db_engine, index=False, if_exists='replace')
    
    
    


