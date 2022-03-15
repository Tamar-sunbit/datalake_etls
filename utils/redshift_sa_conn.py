from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


class RedShiftEngineAlchemy:
    def __init__(self, host, user, passwd):
        self.host = f'{host}.amazonaws.com'
        self.port = 5439
        self.username = user
        self.password = passwd

    def get_url(self, dbname):
        uri_rs = URL.create(drivername='redshift+redshift_connector',
                            # indicate redshift_connector driver and dialect will be used
                            host=self.host,  # Amazon Redshift host
                            port=5439,  # Amazon Redshift port
                            database=dbname,  # Amazon Redshift database
                            username=self.username,
                            password=self.password
                            )
        print(uri_rs)
        return uri_rs

    def get_okta_engine(self, dbname):
        url = URL.create(
            drivername='redshift+redshift_connector',  # indicate redshift_connector driver and dialect will be used
            database=dbname,
            username='tamar.domany@sunbit.com',  # Okta username
            password='Doman11i!i'  # Okta password
        )
        print(url)
        # a dictionary is used to store additional connection parameters
        # that are specific to redshift_connector or cannot be URL encoded.
        conn_params = {
            "iam": True,  # must be enabled when authenticating via IAM
            "credentials_provider": "OktaCredentialsProvider",
            "idp_host": "sunbit.okta.com",
            "app_id": "cpganakgowvv",
            "app_name": "amazon_aws_redshift",
            "region": "us-east-1",
            "cluster_identifier": "redshift-cluster-1",
            "ssl_insecure": False  # ensures certificate verification occurs for idp_host
        }
        # '''redshift-cluster-1.cpganakgowvv.us-east-1.redshift.amazonaws.com:5439/dev'''
        engine = create_engine(url, connect_args=conn_params)
        return engine

    def get_engine(self, dbname):
        url = self.get_url(dbname)
        '''
        conn_params = {
                        "iam": True, # must be enabled when authenticating via IAM
                        "credentials_provider": "OktaCredentialsProvider",
                        "idp_host": "sunbit.okta.com",
                        "cluster_identifier": "redshift-cluster-1",
                        "app_name": "amazon_aws_redshift",
                        "region": "us-east-1",
                        "app_id": "cpganakgowvv",
                        "ssl_insecure": True, # ensures certificate verification occurs for idp_host
                       }
        '''
        engine = create_engine(url)  # , connect_args=conn_params)
        return engine


class LabServerlessConn(RedShiftEngineAlchemy):
    def __init__(self):
        user = 'Adminpass1'
        passwd = 'Adminpass1'
        hostname = '612580448941.us-east-1.redshift-serverless'
        super().__init__(hostname, user, passwd)


class AwsRedshiftConn(RedShiftEngineAlchemy):
    def __init__(self):
        user = 'awsuser'
        passwd = 'yEYrg4fzYA9r_N7tPu!V-8yC!h6GbenR'
        hostname = 'redshift-playground-prod.c7ptte9tecwo.us-west-2.redshift'
        super().__init__(hostname, user, passwd)
