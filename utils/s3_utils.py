import json
import boto3
import pandas as pd
import time
import io
from urllib3 import exceptions
import awswrangler as wr
from botocore.exceptions import ReadTimeoutError

from df_utils import normalize_df


def read_s3_parquet_df(bucket_name, parquet_file_name):
    s3_url = f's3://{bucket_name}/{parquet_file_name}'
    tries = 5
    for i in range(tries):
        try:
            df = wr.s3.read_parquet(path=s3_url, use_threads=True)
        except ReadTimeoutError as pe:
            if i < tries - 1:
                print(f'ReadTimeoutError try {i+1}/{tries} msg {str(pe)}')
                time.sleep(1)
                continue
            else:
                print(f'ReadTimeoutError failed {tries} time raising exception{str(pe)}')
                raise
        break
    return df


def read_s3_str(s3_client, bucket_name, json_file_name):
    tries = 5
    for i in range(tries):
        try:
            s3_clientobj = s3_client.get_object(Bucket=bucket_name, Key=json_file_name)
            s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
        except exceptions.ProtocolError as pe:
            if i < tries - 1:
                print(f'ProtocolError try {i+1}/{tries} msg {str(pe)}')
                time.sleep(1)
                continue
            else:
                print(f'ProtocolError failed {tries} time raising exception{str(pe)}')
                raise
        except exceptions.ReadTimeoutError as rte:
            if i < tries - 1:
                print(f'ProtocolError try {i + 1}/{tries} msg {str(rte)}')
                time.sleep(1)
                continue
            else:
                print(f'ProtocolError failed {tries} time raising exception{str(rte)}')
                raise

        break

    return s3_clientdata


def read_s3_json(s3_client, bucket_name, json_file_name):
    s3_clientdata = read_s3_str(s3_client, bucket_name, json_file_name)
    file_dict = json.loads(s3_clientdata)
    return file_dict


def read_s3_dict_list(s3_client, bucket_name, json_file_name):
    s3_clientdata = read_s3_str(s3_client, bucket_name, json_file_name)
    str_list = s3_clientdata.split('\n')[:-1]
    dict_list = [json.loads(x) for x in str_list]
    df = pd.DataFrame(dict_list)
    return df


def read_mails_s3(s3_client, bucket_name, json_file_name):
    s3_clientdata = read_s3_str(s3_client, bucket_name, json_file_name)
    str_list = s3_clientdata.split('\n')[:-1]
    dict_list = [json.loads(json.loads(x)) for x in str_list]
    df = pd.DataFrame(dict_list)
    return df


def s3_filter_files(s3_bucket_filter, re_pattern, s_date):
    filter_files = []
    for obj in s3_bucket_filter:
        if s_date <= obj.last_modified and re_pattern.match(obj.key.split('/')[-1]):
            #print(f'{obj.key} - {obj.last_modified.strftime("%b %d,%Y %H:%M:%S")}')
            filter_files.append(obj.key)
    return filter_files
