import os
import boto3
import configparser


class S3Credentials:
    def __init__(self, ):
        config = configparser.RawConfigParser()
        path = os.path.join(os.path.expanduser('~'), '.aws/credentials')
        config.read(path)
        self.aws_access_key_id = config.get('default', 'aws_access_key_id')
        self.aws_secret_access_key = config.get('default', 'aws_secret_access_key')

    def get_aws_access_key_id(self):
        return self.aws_access_key_id

    def get_aws_secret_access_key(self):
        return self.aws_secret_access_key


class S3BotoBucket(S3Credentials):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
        self.s3_bucket = self.resource.Bucket(self.bucket_name)

    def get_s3_client(self):
        return self.client

    def get_s3_resource(self):
        return self.s3_resource

    def get_s3_bucket(self):
        return self.s3_bucket

    def get_bucket_name(self):
        return self.bucket_name
