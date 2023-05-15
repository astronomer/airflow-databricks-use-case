import csv

# --------- AWS S3 specific --------- #
import boto3

# --------- /AWS S3 specific -------- #
from io import StringIO
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

# set variables
ACCESS_KEY = "<your AWS Access Key ID>"  # dbutils.secrets.get(scope="my-scope", key="my-aws-key-key")
SECRET_KEY = "<your AWS Secret Access Key>"  # dbutils.secrets.get(scope="my-scope", key="my-aws-secret-key")
BUCKET_NAME = "databricks-tutorial-bucket"
S3_FOLDER_JOINED_DATA = "joined_data"
S3_FOLDER_TRANSFORMED_DATA = "transformed_data"

# --------- AWS S3 specific -------- #
# list files in the `joined_data` directory of your S3 bucket
s3 = boto3.client("s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"{S3_FOLDER_JOINED_DATA}/")
csv_files = [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".csv")]
# --------- /AWS S3 specific -------- #

# load data from CSVs in the `joined_data` folder into separate Spark dataframes
dfs = []
for file in csv_files:
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=file)
    body = obj["Body"].read().decode("utf-8")
    csv_reader = csv.reader(StringIO(body), delimiter=",", quotechar='"')
    header = next(csv_reader)
    schema = StructType([StructField(col, StringType(), True) for col in header])
    df = spark.createDataFrame(csv_reader, schema)
    dfs.append(df)

# calculate summation column for each Spark dataframe
dfs_transformed = []
for df in dfs:
    df = df.withColumn(
        "SHW%",
        col("Solar (% electricity)")
        + col("Hydro (% electricity)")
        + col("Wind (% electricity)"),
    )
    dfs_transformed.append(df)

# convert each spark dataframe into pandas and load to S3
for file_name, df in zip(csv_files, dfs_transformed):
    # Convert Spark DataFrame to Pandas DataFrame and write to in memory CSV file
    pandas_df = df.toPandas()
    csv_buffer = StringIO()
    pandas_df.to_csv(csv_buffer, index=False)

    # --------- AWS S3 specific --------- #
    # upload data to S3
    s3 = boto3.client(
        "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
    )
    s3.put_object(
        Body=csv_buffer.getvalue(),
        Bucket=BUCKET_NAME,
        Key=f"{S3_FOLDER_TRANSFORMED_DATA}/{file_name.split('/')[1]}",
    )
    # --------- /AWS S3 specific -------- #
