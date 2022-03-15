import numpy as np
import pandas as pd
import re
from datetime import datetime
import pytz
from redshift_sa_conn import AwsRedshiftConn
from s3_boto_conect import S3BotoBucket
from s3_utils import read_mails_s3, s3_filter_files
from df_utils import normalize_df
from sql_utils import df_copy_table


def iterable_to_df(s3_client, file_list, s3_bucket_name, norm_cols, s_date):
    cnt = 0

    all_df = pd.DataFrame()
    for obj in file_list:
        print(f'reading {obj} - {cnt}/{len(file_list)}')
        df1 = read_mails_s3(s3_client, s3_bucket_name, obj)
        df1 = normalize_df(df1, norm_cols)
        # Change userID from string to int and dates from string to datetime
        print(f'removing {df1[df1["userId"].isnull()].shape[0]} record with no userId')
        df1 = df1[df1['userId'].notnull()]
        df1.loc[df1['userId'].notnull(), 'userId'] = df1[df1['userId'].notnull()]['userId'].astype(int)
        df1 = df1.replace({None: np.NAN})
        df1.loc[:, 'createdAt'] = pd.to_datetime(df1['createdAt']).dt.tz_localize(None)
        df1 = df1[df1['createdAt'] >= s_date.replace(tzinfo=None)]
        if 'profileUpdatedAt' in df1.columns:
            df1.loc[:, 'profileUpdatedAt'] = pd.to_datetime(df1['profileUpdatedAt']).dt.tz_localize(None)
        all_df = all_df.append(df1)
        cnt += 1
        print(f'Adding {df1.shape[0]} records from s3 file number {cnt}')

    print(cnt)
    print(all_df.shape[0])
    ren_dict = {x: x.replace('.', '_').replace('-', '_') for x in all_df.columns}
    all_df.rename(columns=ren_dict, inplace=True)
    return all_df


def import_iterable_mails(s3_bucket_class, start_date, db_eng, schem):
    table_name = 'iterable_events'
    norm_cols = ['dataFields']
    super_cols = ['labels', 'transactionalData', 'messageTypeIds', 'emailListIds', 'channelIds']

    exists = False
    prefix = 'iterable-events/streaming_events/iterable.events/partition'
    pattern = re.compile('.*\.json$')
    for partition in range(3, 25):
        part_prefix = f'{prefix}={partition}/'
        files = s3_filter_files(s3_bucket_class.get_s3_bucket().objects.filter(Prefix=part_prefix), pattern, start_date)
        mail_df = iterable_to_df(s3_bucket_class.get_s3_client(), files, s3_bucket_class.get_bucket_name(), norm_cols, start_date)
        df_copy_table(db_eng, schem, table_name, mail_df, super_fields=super_cols, exists=True)
        exists = True


if __name__ == "__main__":
    dbname = 'dev'
    schema = 'silver'
    db_conn = AwsRedshiftConn()
    db_engine = db_conn.get_engine(dbname)

    bucket_name = 'sunbit-oregon-prod-datalake'
    my_bucket = S3BotoBucket(bucket_name)

    start_datetime = datetime(2022, 1, 1, 0, 0, 0, 0, pytz.UTC)

    import_iterable_mails(my_bucket, start_datetime, db_engine, schema)
