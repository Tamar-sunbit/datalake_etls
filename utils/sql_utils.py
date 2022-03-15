import pandas as pd
import numpy as np
import re
import json
import time
import boto3
import awswrangler as wr
import datetime
from sqlalchemy import exc, text
from sqlalchemy_redshift.dialect import SUPER, VARCHAR
import pandas_redshift as pr
from s3_boto_conect import S3Credentials


def df_drop_table(db_engine, schema, table_name):
    drop_query = f'DROP TABLE IF EXISTS "{schema}"."{table_name}";'
    db_engine.execute(drop_query)


def df_create_table(db_engine, schema, table_name, df, key_fields=None, super_fields=None):
    url_fields = [x for x in df.columns if 'url' in x.lower()]
    dtype_dict = {}
    if len(url_fields) > 0:
        dtype_dict.update({c: VARCHAR(2048) for c in url_fields})
    if super_fields:
        dtype_dict.update({c: SUPER for c in super_fields})
    if not bool(dtype_dict):
        dtype_dict = None
    create_query = pd.io.sql.get_schema(df, table_name, keys=key_fields, con=db_engine, dtype=dtype_dict)
    create_query = create_query.replace(table_name, f'{schema}.{table_name}')
    create_query = create_query.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
    print(create_query)
    db_engine.execute(create_query)


def df_drop_and_create_table(db_engine, schema, table_name, df, key_fields=None, super_fields=None):
    df_drop_table(db_engine, schema, table_name)
    df_create_table(db_engine, schema, table_name, df, key_fields, super_fields)


def is_exists(db_engine, schem, table_name):
    q1 = f"SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schem}' AND TABLE_NAME = '{table_name}';"
    res = db_engine.execute(q1)
    result_as_list = res.fetchall()
    return result_as_list[0]['count']


def get_insert_sql_from_df(df, schem, dest_table, super_cols):
    insert = f'INSERT INTO "{schem}"."{dest_table}" '

    columns_string = str(list(df.columns))[1:-1]
    columns_string = re.sub(r' ', '\n        ', columns_string)
    columns_string = re.sub(r"\'", "", columns_string)
    #df[col].apply(lambda x: np.nan if bool(x) else x),,
    for col in super_cols:
        df.loc[:, col] = df[col].apply(lambda x: np.nan if x == x and (len(x) == 0 or x=='{}') else x) # replace empty list/dict with nan
        df.loc[:, col] = df[col].apply(lambda x: "JSON_PARSE('" + json.dumps(x) + "')REM" if x==x else x)

    for col in df.select_dtypes(include=[object]).columns:
        if col not in super_cols:
            df.loc[:, col] = df[col].astype(str).str.replace("'", 'CHR(39)')
            # df.loc[:, col] = df[col].str[0:255]
            df.loc[:, col] = df[col].str.replace("%", '%%')

    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df.loc[:, col] = df[col].dt.strftime('%b %d,%Y %H:%M:%S')

    records = list(df.to_records(index=False))
    values_string = re.sub(r'nan', 'null', str(records))
    values_string = re.sub(r"'null'", "null", values_string)
    # values_string = re.sub(r'None', 'null', str(values_string))

    # values_string = re.sub(r"\'", "\"", values_string)
    # values_string = re.sub(r"'JSON_PARSE", "JSON_PARSE", values_string)
    # values_string = re.sub(r")REM'", ")", values_string)
    values_string = values_string.replace("CHR(39)", r"\'")
    values_string = values_string.replace(r"\')REM'", "')").replace(')REM"', ')')
    values_string = values_string.replace(r"'JSON_PARSE(\'", "JSON_PARSE('").replace('"JSON_PARSE', 'JSON_PARSE')
    values_string = values_string[1:-1].replace('), ', '),\n')

    return f"{insert} ({columns_string}) \n VALUES\n {values_string};"


def df_insert_table(db_engine, schem, table_name, df, key_fields=None, super_fields=None, exists=False):
    #df = df.replace({None: np.NAN})
    #if is_exists(db_engine, schem, table_name) == 0:
    if not exists:
        df_create_table(db_engine, schem, table_name, df, key_fields, super_fields)

    lop = 0
    chunk = 5 # 00
    for i in range(0, df.shape[0], chunk):
        insert_query = get_insert_sql_from_df(df.iloc[i:i+chunk], schem, table_name, super_cols=super_fields)
        print(insert_query)
        tries = 5
        for j in range(tries):
            try:
                db_engine.execute(insert_query)
                print(f'{i}/{df.shape[0]}) rows added to {table_name} (try {lop})')
                break
            except TimeoutError as te:
                if j < tries - 1:
                    print(f'TimeoutError try {j}/{tries} msg {str(te)}')
                    time.sleep(1)
                    continue
            except exc.DBAPIError as err:
                if j < tries - 1:
                    print(f'DBAPIError try {j}/{tries} msg {str(err)}')
                    time.sleep(1)
                    continue
            except BaseException as e:
                if j < tries - 1:
                    print(f'BaseException try {j}/{tries} msg {str(e)}')
                    time.sleep(1)
                    continue


def df_copy_table(db_engine, schem, table_name, df, key_fields=None, super_fields=None, exists=False):
    temp_bucket_name ='sunbit-oregon-staging-dl-bronze-layer'
    s3_cred = S3Credentials()
    file_name = f'temp_csv_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    s3_file = f's3://{temp_bucket_name}/wr-temp-folder/{file_name}'

    for col in super_fields:
        df.loc[:, col] = df[col].apply(lambda x: np.nan if x == x and (len(x) == 0 or x=='{}') else x) # replace empty list/dict with nan
        df.loc[:, col] = df[col].apply(lambda x: json.dumps(x) if x==x else x)

    if not exists:
        df_create_table(db_engine, schem, table_name, df, key_fields, super_fields)

    df.to_csv(s3_file, index=False)
    copy_string = f'''COPY {schem}.{table_name} ({','.join(df.columns.tolist())}) FROM '{s3_file}' CREDENTIALS
                 'aws_access_key_id={s3_cred.get_aws_access_key_id()};aws_secret_access_key={s3_cred.get_aws_secret_access_key()}'
                 IGNOREHEADER 1
                 CSV;
                 '''
    print(copy_string)
    res = db_engine.execute(text(copy_string).execution_options(autocommit=True))
    #s3 = boto3.client('s3')
    #s3.delete_object(Bucket=temp_bucket_name, Key=file_name)
