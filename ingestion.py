import json
import time

import boto3
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName('Your app name')\
    .config('spark.sql.adaptive.enabled', True)\
    .getOrCreate()
sc = spark.sparkContext

class GetS3Data:
    def __init__(self):
        self.client = boto3.client(
            'athena',
            aws_access_key_id = self.get_access_key(),
            aws_secret_access_key = self.get_secret_key(),
            region_name= 'us-east-1'
        )
        self.query_id = self.start_execution()['QueryExecutionId']
        self.df = None

    def get_access_key(self):
        return 'Your access key'

    def get_secret_key(self):
        return 'Your secret key'

    def start_execution(self):
        return self.client.start_query_execution(
            QueryString= 'SELECT * FROM your_database',
            QueryExecutionContext={
                'Database': 'your_database'
            },
            WorkGroup= 'your_work_group'
        )

    def run(self):
        try:
            self.check_execution()
            self.write()
        except Exception as e:
            msg = f'Ingestion of your data failed to run with the following error {e}'
            raise Exception(msg) from Exception

    def check_execution(self):
        query_status = None
        TEN_MINUTES = 60*10
        timeout = time.time() + TEN_MINUTES
        CONTINUE_STATUSES = ['QUEUED', 'RUNNING']
        INGESTION_FAILED_STATUSES = ['FAILED', 'CANCELLED']

        while query_status in CONTINUE_STATUSES or query_status is None:
            query_status = self.client.get_query_execution(QueryExecutionId=self.query_id)['QueryExecution']['Status']['State']
            if query_status in INGESTION_FAILED_STATUSES or time.time() > timeout:
                raise Exception('Interact Analytics query failed or was cancelled.')
            time.sleep(0.5)
            
    def write(self):
        self.get_query_results()\
            .write\
            .mode('append')\
            .format('json')\
            .save('your/file/path')

    def get_query_results(self):
        response = self.client.get_query_results(
            QueryExecutionId=self.query_id
        )
        output = None
        while 'NextToken' in response:
            response = self.client.get_query_results(
                QueryExecutionId = self.query_id,
                NextToken = response['NextToken']
            )
            try:
                response_as_json = json.dumps(response['ResultSet'])
                _df = spark.read.json(sc.parallelize([response_as_json]))
                output = output.union(_df)
            except Exception:
                response_as_json = json.dumps(response['ResultSet'])
                output = spark.read.json(sc.parallelize([response_as_json]))
        else:
            response_as_json = json.dumps(response['ResultSet'])
            _df = spark.read.json(sc.parallelize([response_as_json]))
            try:
                output = output.union(_df)
            except:
                output = _df
        return output
