
import urllib.request
from pathlib import Path
import pandas as pd, re

from metaflow import FlowSpec, step, Parameter

class MainDataFlow(FlowSpec):

    @step
    def start(self):
        import boto3
        from os.path import join
        import urllib.request
        
        url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/00560/SeoulBikeData.csv'
        self.bucket = "datalake"
        self.raw_dir = "raw"
        self.raw_data_name = 'SeoulBikeData_test.csv'
        self.tmp_file, _ = urllib.request.urlretrieve(url, self.raw_data_name)

        s3_resource = boto3.resource("s3", endpoint_url="http://localhost.localstack.cloud:4566")         
        s3_resource.Object(bucket_name=self.bucket, key=join(self.raw_dir, self.raw_data_name)).upload_file(self.tmp_file)

        self.next(self.extract)

    @step
    def extract(self):
        import pandas as pd
        
        # get the data with its peculiar encoding
        self.data = pd.read_csv(self.tmp_file, encoding='iso-8859-1', parse_dates=['Date'], infer_datetime_format=True)
        self.next(self.transform)


    @step
    def transform(self):
        self.data.columns = [re.sub(r'[^a-zA-Z0-9\s_]', '', col).lower().replace(r" ", "_") for col in self.data.columns]

        self.data.sort_values(['date', 'hour'], inplace=True)

        self.data["year"]           = self.data['date'].dt.year
        self.data["month"]          = self.data['date'].dt.month
        self.data["week"]           = self.data['date'].dt.isocalendar().week
        self.data["day"]            = self.data['date'].dt.day
        self.data["day_of_week"]    = self.data['date'].dt.dayofweek
        self.data["is_month_start"] = self.data['date'].dt.is_month_start
        
        self.data.drop('date', axis=1, inplace=True)

        self.data = pd.get_dummies(data=self.data, columns=['holiday', 'seasons', 'functioning_day'])

        self.next(self.load)

    @step
    def load(self):
        import boto3
        from os.path import join
        from tempfile import TemporaryDirectory

        self.interim = "interim"
        self.clean_data_name = "clean22.parquet"

        with TemporaryDirectory("hello_temp") as tmp:
            tmp_file = join(tmp, self.clean_data_name)
            self.data.to_parquet(tmp_file)
            s3_resource = boto3.resource("s3", endpoint_url="http://localhost.localstack.cloud:4566")         
            s3_resource.Object(bucket_name=self.bucket, key=join(self.interim, self.clean_data_name)).upload_file(tmp_file)
        
        self.next(self.end)

    @step
    def end(self):
        print("MainDataFlow has finished successfully!")

if __name__ == "__main__":
    MainDataFlow()
