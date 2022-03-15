import pandas as pd
import re
from datetime import datetime
import pytz
from redshift_sa_conn import AwsRedshiftConn
from s3_boto_conect import S3BotoBucket
from s3_utils import read_s3_dict_list, read_mails_s3, s3_filter_files
from df_utils import normalize_df
from sql_utils import df_insert_table, df_copy_table


def mailgun_s3_to_df(s3_client, file_list, s3_bucket_name, norm_cols, s_date):
    relevant_tags = ['com.sunbit.notification.purchase.payment.OnSchedule',
                     'com.sunbit.notification.purchase.PaidOff',
                     'com.sunbit.notification.purchase.Agreement.TAB']
    cnt = 0
    all_mails_df = pd.DataFrame()
    for obj in file_list:
        print(f'reading {obj} - {cnt}/{len(file_list)}')
        df1 = read_mails_s3(s3_client, s3_bucket_name, obj)
        df1 = normalize_df(df1, norm_cols)
        tags_df = pd.DataFrame(df1['tags'].to_list())
        cond = (False)
        for i in range(0, tags_df.shape[1]):
            cond = cond | (tags_df[i].isin(relevant_tags))
        df2 = df1[cond]
        df2 = df2[df2['timestamp'] >= s_date.timestamp()]
        df2.loc[:, 'timestamp1'] = pd.to_datetime(df2['timestamp'], unit='s').dt.tz_localize(None)
        df2.drop(columns=['timestamp'], inplace=True)
        all_mails_df = all_mails_df.append(df2)
        print(f'{df2.shape[0]}/{df1.shape[0]} records appended ')
        print(all_mails_df.shape[0])

        cnt += 1
        #if cnt>10:
        #    break
    print(cnt)
    print(all_mails_df.shape[0])
    ren_dict = {x: x.replace('.', '_').replace('-', '_') for x in all_mails_df.columns}
    all_mails_df.rename(columns=ren_dict, inplace=True)
    return all_mails_df


def import_mailgun_delivered_mails(s3_bucket_class, start_date, db_eng, schem):
    table_name = 'mailgun_delivered_events'
    norm_cols = ['delivery-status', 'message']
    super_cols = ['tags', 'storage', 'campaigns', 'user_variables', 'flags', 'envelope', 'attachments']

    # add compressed files (one by one)
    comp_prefix = 'mailgun-events/streaming_events/mailgun.delivered/compacted/'
    cpmp_pattern = re.compile('^2022-03+.*\.json$')
    comp_files = s3_filter_files(s3_bucket_class.get_s3_bucket().objects.filter(Prefix=comp_prefix), cpmp_pattern, start_date)
    exists = False

    skip = []
    for p in range(0, 6):
        skip.append(f'mailgun-events/streaming_events/mailgun.delivered/compacted/2022-03-01-s3__sunbit-oregon-prod-datalake_mailgun-events_streaming_events_mailgun.delivered_partition={p}.json')

    for cf in comp_files:
        if cf in skip:
            continue
        mail_df = mailgun_s3_to_df(s3_bucket_class.get_s3_client(), [cf], s3_bucket_class.get_bucket_name(), norm_cols, start_date)
        df_copy_table(db_eng, schem, table_name, mail_df, super_fields=super_cols, exists=exists)
        exists = True

    prefix = 'mailgun-events/streaming_events/mailgun.delivered/partition'
    pattern = re.compile('^mailgun+.*\.json$')
    for partition in range(0, 25):
        part_prefix = f'{prefix}={partition}'
        files = s3_filter_files(s3_bucket_class.get_s3_bucket().objects.filter(Prefix=part_prefix), pattern, start_date)
        mail_df = mailgun_s3_to_df(s3_bucket_class.get_s3_client(), files, s3_bucket_class.get_bucket_name(), norm_cols, start_date)
        #df_insert_table(db_eng, schem, table_name, mail_df, super_fields=super_cols, exists=exists)
        df_copy_table(db_eng, schem, table_name, mail_df, super_fields=super_cols, exists=exists)
        exists = True

    '''
    first = True
    delta = timedelta(days=7)
    last_date = datetime.now(pytz.UTC) + delta # for mails arrived during process
    bucket_filter = s3_bucket.objects.filter(Prefix=prefix)
    while start_date <= last_date:
        end_date = start_date + delta
        print(f'start date {start_date.strftime("%b %d,%Y %H:%M:%S")} - {end_date.strftime("%b %d,%Y %H:%M:%S")}')

        mail_df = mailgun_s3_to_df(s3_client, bucket_filter, s3_bucket.name, norm_cols, start_date, end_date)
        if not mail_df.empty:
            df_insert_table(db_eng, schem, table_name, mail_df, super_fields=super_cols, exists=first)
        start_date = end_date
        first = False
    '''


def import_mailgun_opened_mails(s3_bucket_class, start_date, db_eng, schem):
    table_name = 'mailgun_opened_events'
    norm_cols = ['message']
    super_cols = ['geolocation', 'tags', 'campaigns', 'user_variables', 'client_info']

    exists = False
    prefix = 'mailgun-events/streaming_events/mailgun.opened/partition'
    pattern = re.compile('^mailgun+.*\.json$')
    for partition in range(0, 25):
        part_prefix = f'{prefix}={partition}'
        files = s3_filter_files(s3_bucket_class.get_s3_bucket().objects.filter(Prefix=part_prefix), pattern, start_date)
        mail_df = mailgun_s3_to_df(s3_bucket_class.get_s3_client(), files, s3_bucket_class.get_bucket_name(), norm_cols, start_date)
        df_copy_table(db_eng, schem, table_name, mail_df, super_fields=super_cols, exists=exists)
        exists = True


def import_mailgun_clicked_mails(s3_client, s3_bucket, start_date, db_eng, schem):
    prefix = 'mailgun-events/streaming_events/mailgun.clicked/partition='
    table_name = 'mailgun_clicked_events'
    norm_cols = ['message']
    super_cols = ['geolocation', 'tags', 'campaigns', 'user_variables', 'client_info']

    mail_df = mailgun_s3_to_df(s3_client, s3_bucket.objects.filter(Prefix=prefix), s3_bucket.name, norm_cols,
                               start_date)
    df_insert_table(db_eng, schem, table_name, mail_df, super_fields=super_cols, exists=False)


def import_mailgun_failed_mails(s3_client, s3_bucket, start_date, db_eng, schem):
    prefix = 'mailgun-events/streaming_events/mailgun.failed_permanent/partition='
    table_name = 'mailgun_failed_events'
    norm_cols = ['delivery-status', 'message']
    super_cols = ['tags', 'storage', 'campaigns', 'user_variables', 'flags', 'envelope', 'attachments']

    mail_df = mailgun_s3_to_df(s3_client, s3_bucket.objects.filter(Prefix=prefix), s3_bucket.name, norm_cols,
                               start_date)
    df_insert_table(db_eng, schem, table_name, mail_df, super_fields=super_cols, exists=False)


if __name__ == "__main__":
    dbname = 'dev'
    schema = 'silver'
    db_conn = AwsRedshiftConn()
    db_engine = db_conn.get_engine(dbname)

    bucket_name = 'sunbit-oregon-prod-datalake'
    my_bucket = S3BotoBucket(bucket_name)

    start_datetime = datetime(2022, 1, 1, 0, 0, 0, 0, pytz.UTC)


    import_mailgun_delivered_mails(my_bucket, start_datetime,  db_engine, schema)
    #import_mailgun_opened_mails(s3_client, my_bucket, start_datetime, db_engine, schema)
    #import_mailgun_clicked_mails(s3_client, my_bucket, start_datetime, db_engine, schema)
    #import_mailgun_failed_mails(s3_client, my_bucket, start_datetime, db_engine, schema)
    print('DONE')


    #for i, cnt in pd.DataFrame(all_mails_df['tags'].to_list())[0].value_counts().items():
    #    if 'card' in i:
    #        print(i, cnt)
