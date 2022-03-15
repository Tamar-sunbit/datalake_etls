import numpy as np
import pandas as pd
import re
from datetime import datetime
import pytz
import boto3
import json
from redshift_sa_conn import AwsRedshiftConn
from s3_utils import read_s3_dict_list, read_mails_s3, read_s3_parquet_df, s3_filter_files
from df_utils import normalize_df
from sql_utils import df_insert_table


def read_mongo_notification(bucket_name, file_name):
    paq_df = read_s3_parquet_df(bucket_name, file_name)
    paq_df.loc[:, '_doc'] = paq_df['_doc'].apply(lambda x: json.loads(x))
    paq_norm_df = normalize_df(paq_df, ['_doc'])
    paq_norm_df.loc[:, 'notificationData'] = paq_norm_df['notificationData'].apply(lambda x: json.loads(x))
    paq_norm_df.loc[:, 'notificationType'] = paq_norm_df['notificationData'].apply(lambda x: x['notificationType'])
    #paq_norm_df = normalize_df(paq_norm_df, ['notificationData'])
    return paq_norm_df


def import_mongo_mails(s3_client, s3_bucket, start_date, db_eng, schem):
    prefix = 'mongodb_raw/NotificationService/NotificationsHistory/20220101'
    relevant_tags = ['com.sunbit.notification.purchase.payment.OnSchedule',
                     'com.sunbit.notification.purchase.PaidOff',
                     'com.sunbit.notification.purchase.Agreement.TAB',
                     'com.sunbit.notification.card.iterable.invitation']
    pattern = re.compile('^20220101+.*\.parquet$')
    files = s3_filter_files(s3_bucket.objects.filter(Prefix=prefix), pattern, start_date)
    print(f'{len(files)} files with prefix = {prefix}')

    cnt = 0

    all_df = pd.DataFrame()
    #filtered_files = s3_bucket.objects.filter(Prefix=prefix)
    for obj in files:
        print('{0}:{1}'.format(s3_bucket.name, obj))
        df1 = read_mongo_notification(s3_bucket.name, obj)
        df2 = df1[df1['notificationType'].isin(relevant_tags)]
        df2.loc[:, 'localDateTime.$date'] = pd.to_datetime(df2['localDateTime.$date'], unit='ms').dt.tz_localize(None)
        df2 = df2[df2['localDateTime.$date'] >= start_date.replace(tzinfo=None)]
        #df2.loc[:, 'creationDate'] = pd.to_datetime(df2['creationDate']).dt.tz_localize(None)
        all_df = all_df.append(df2)
        #iterable_mails_to_db(db_eng, schem, df1, exists=bool(cnt))
        print(f'Adding {df2.shape[0]} records from s3 file number {cnt}')
        print(all_df.shape[0])

        cnt += 1
        if cnt >= 3:
            break
        print(cnt)
    print(all_df.shape[0])
    ren_dict = {x: x.lower().replace('.', '_').replace('-', '_').replace('$', '') for x in all_df.columns}
    all_df.rename(columns=ren_dict, inplace=True)

    #Add to DB after reading all files to know the final schema
    print(f'Adding {all_df.shape[0]} records to DB')
    mongo_notifications_to_db(db_eng, schem, all_df, exists=False)


def mongo_notifications_to_db(db_eng, schem, mon_df, exists):
    table_name = 'mongodb_events'
    super_cols = ['recipient'] #, 'notificationdata']
    #super_cols = ['recipient', 'notificationpartlist', 'notificationdata_recipient']
    mon_df.drop(columns=['notificationpartlist', 'notificationdata'], inplace=True)
    df_insert_table(db_eng, schem, table_name, mon_df, super_fields=super_cols, exists=exists)


if __name__ == "__main__":
    dbname = 'dev'
    schema = 'silver'
    db_conn = AwsRedshiftConn()
    db_engine = db_conn.get_engine(dbname)
    bucket_name = 'sunbit-oregon-prod-datalake'

    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    my_bucket = s3_resource.Bucket(bucket_name)
    start_datetime = datetime(2022, 1, 1, 0, 0, 0, 0, pytz.UTC)

    import_mongo_mails(s3_client, my_bucket, start_datetime, db_engine, schema)


