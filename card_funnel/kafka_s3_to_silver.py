import numpy as np
import pandas as pd
from redshift_sa_conn import AwsRedshiftConn
from s3_utils import read_s3_dict_list
from sql_utils import df_create_table, df_insert_table


def get_file_df(bucket, json_file_name, norm_field):
    app_df = read_s3_dict_list(bucket, json_file_name)
    app_df = pd.concat([app_df, pd.json_normalize(app_df[norm_field])], axis=1)
    return app_df


def topic_json_to_sql(db_eng, schem, topic_df):
    table_name = f'{application_df["topic"].value_counts().idxmax().replace(".", "_")}_topic'
    msg_cols = ["topic", "partition", "offset", "key", "value"]
    vals_cols = [x for x in application_df.columns if x not in msg_cols]
    df_insert_table(db_eng, schem, table_name, topic_df[vals_cols])


if __name__ == "__main__":
    dbname = 'dev'
    schema = 'silver'
    db_conn = AwsRedshiftConn()
    db_engine = db_conn.get_engine(dbname)

    application_df = get_file_df('datalake-test-tamar-bucket', 'card.applications.json', 'value')
    # If more than one record with same ID and status take the latest (timestamp) one
    application_df = application_df.sort_values('timestamp')
    application_df = application_df.drop_duplicates(['cardApplicationId', 'applicationStatus'], keep='last')

    temp_app_df = application_df[~application_df['applicationStatus'].isin(['INITIAL', 'DEPRECATED'])]

    invitations_df = get_file_df('datalake-test-tamar-bucket', 'card.application.invitation.json', 'value')

    elig_df = get_file_df('datalake-test-tamar-bucket', 'card.eligible.customer.application.card.json', 'value')

    topic_json_to_sql(db_engine, schema, application_df)

    print()
