import redshift_connector


class RedShiftConn:
    def __init__(self, host, user, passwd, dbname):
        self.host = f'{host}.redshift.amazonaws.com'
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