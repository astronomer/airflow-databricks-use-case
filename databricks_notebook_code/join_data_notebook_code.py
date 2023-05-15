# package imports
import csv

# --------- AWS S3 specific --------- #
import boto3

# --------- /AWS S3 specific -------- #
from io import StringIO
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

# set variables
ACCESS_KEY = "<your AWS Access Key ID>"  # dbutils.secrets.get(scope="my-scope", key="my-aws-key-key")
SECRET_KEY = "<your AWS Secret Access Key>"  # dbutils.secrets.get(scope="my-scope", key="my-aws-secret-key")
BUCKET_NAME = "databricks-tutorial-bucket"
S3_FOLDER_COUNTRY_SUBSET = "country_subset"
S3_FOLDER_JOINED_DATA = "joined_data"

# --------- AWS S3 specific -------- #
# list files in the `country_subset` directory of your S3 bucket
s3 = boto3.client("s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"{S3_FOLDER_COUNTRY_SUBSET}/")
csv_files = [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".csv")]
# --------- /AWS S3 specific ------- #

# load data from CSVs in the `country_subset` folder into separate Spark dataframes
dfs = []
for file in csv_files:
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file)
    body = obj["Body"].read().decode("utf-8")
    csv_reader = csv.reader(StringIO(body), delimiter=",", quotechar='"')
    header = next(csv_reader)
    schema = StructType([StructField(col, StringType(), True) for col in header])
    df = spark.createDataFrame(csv_reader, schema)
    dfs.append(df)

# collect name of the country assessed
entity_name = dfs[0].select("Entity").distinct().collect()[0]["Entity"]
# define results table
schema = StructType(
    [
        StructField("Year", IntegerType(), True),
    ]
)
result_df = spark.createDataFrame([], schema)

# join data tables to result table
for df in dfs:
    col_name = df.columns[3]
    df = df.select("Year", col_name)
    result_df = result_df.join(df, "Year", "outer")

# convert spark dataframe to pandas and remove potentially duplicated columns
pandas_df = result_df.toPandas()
df_t = pandas_df.T
df_t = df_t.loc[~df_t.index.duplicated(keep="first")]
df_clean = df_t.T
csv_buffer = StringIO()
df_clean.to_csv(csv_buffer, index=False)

# --------- AWS S3 specific --------- #
# upload data as a CSV file to S3
s3 = boto3.client("s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
s3.put_object(
    Body=csv_buffer.getvalue(),
    Bucket=BUCKET_NAME,
    Key=f"{S3_FOLDER_JOINED_DATA}/{entity_name}.csv",
)
# --------- /AWS S3 specific -------- #
