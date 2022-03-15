import os
from redshift_sa_conn import AwsRedshiftConn


if __name__ == "__main__":
    db_conn = AwsRedshiftConn()
    dbname = 'dev'
    schema = 'gold'
    prefix = 'card_funnel'
    db_engine = db_conn.get_engine(dbname)

    path = os.path.join(os.getcwd(), 'sqls', 'create_customers_table.sql')
    file = open(path)
    sql_commands = file.read()
    # escaped_sql = sa.text(file.read())
    print(sql_commands)
    stats = sql_commands.split(';')[:-1]
    for sts in stats:
        sql_stat = sts.format(schem=schema, prefix=prefix).strip()
        print(f'!!{sql_stat}!!')
        db_engine.execute(sql_stat)
