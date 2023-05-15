from airflow.decorators import dag
from pendulum import datetime
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table
from astro_databricks.operators.notebook import DatabricksNotebookOperator
from astro_databricks.operators.workflow import DatabricksWorkflowTaskGroup
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

S3_BUCKET = "tutorialtjf231942-s3-bucket"
SOLAR_CSV_PATH = "include/share-electricity-solar.csv"
HYDRO_CSV_PATH = "include/share-electricity-hydro.csv"
WIND_CSV_PATH = "include/share-electricity-wind.csv"
COUNTRY = "Switzerland"
DATABRICKS_RESULT_FILE_PATH = f"s3://{S3_BUCKET}/transformed_data/{COUNTRY}.csv"
DATABRICKS_CONN_ID = "databricks_conn"

S3_FOLDER_COUNTRY_SUBSET = "country_subset"

job_cluster_spec = [
    {
        "job_cluster_key": "tutorial-cluster",
        "new_cluster": {
            "cluster_name": "",
            "spark_version": "11.3.x-scala2.12",
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "zone_id": "eu-central-1",
                "spot_bid_price_percent": 100,
                "ebs_volume_count": 0,
            },
            "node_type_id": "i3.xlarge",
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "enable_elastic_disk": False,
            "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
            "runtime_engine": "STANDARD",
            "num_workers": 1,
        },
    }
]


@aql.transform
def select_countries(in_table, country):
    return """SELECT * FROM {{ in_table }} WHERE "Entity" = {{ country }}"""


@aql.dataframe
def create_graph(df: pd.DataFrame):
    sns.set_style("whitegrid")
    sns.lineplot(x="Year", y="SHW%", data=df)
    plt.title(f"% of Solar, Hydro and Wind in {COUNTRY}")
    plt.xlabel("Year")
    plt.ylabel("Combined SHW (in %)")
    plt.savefig("include/shw.png")


@dag(start_date=datetime(2023, 1, 1), schedule=None, catchup=False)
def renewable_analysis_dag():
    in_tables = aql.LoadFileOperator.partial(
        task_id="in_tables",
        output_table=Table(
            conn_id="postgres_conn",
        ),
    ).expand(
        input_file=[
            File(path=SOLAR_CSV_PATH),
            File(path=HYDRO_CSV_PATH),
            File(path=WIND_CSV_PATH),
        ]
    )

    country_tables = select_countries.partial(country=COUNTRY).expand(
        in_table=in_tables.output
    )

    save_files_to_S3 = aql.ExportToFileOperator.partial(
        task_id="save_files_to_S3",
        if_exists="replace",
    ).expand_kwargs(
        country_tables.map(
            lambda x: {
                "input_data": x,
                "output_file": File(
                    path=f"s3://{S3_BUCKET}/{S3_FOLDER_COUNTRY_SUBSET}/{x.name}.csv",
                    conn_id="aws_conn",
                ),
            }
        )
    )

    task_group = DatabricksWorkflowTaskGroup(
        group_id="test_workflow",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_clusters=job_cluster_spec,
    )

    with task_group:
        notebook_1 = DatabricksNotebookOperator(
            task_id="join_data",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/tamara.fingerlin@gmail.com/join_data",
            source="tutorialtjf231942",
            job_cluster_key="tutorial-cluster",
        )
        notebook_2 = DatabricksNotebookOperator(
            task_id="transform_data",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/tamara.fingerlin@gmail.com/transform_data",
            source="tutorialtjf231942",
            job_cluster_key="tutorial-cluster",
        )
        notebook_1 >> notebook_2

    delete_intake_files_S3 = S3DeleteObjectsOperator(
        task_id="delete_intake_files_S3",
        bucket=S3_BUCKET,
        prefix="renewable_analysis/",
        aws_conn_id="aws_conn",
    )

    load_file_to_db = aql.load_file(
        input_file=File(path=DATABRICKS_RESULT_FILE_PATH, conn_id="aws_conn"),
        output_table=Table(conn_id="postgres_conn"),
    )

    (
        save_files_to_S3
        >> task_group
        >> [load_file_to_db, delete_intake_files_S3]
        >> create_graph(load_file_to_db)
    )

    aql.cleanup()


renewable_analysis_dag()
