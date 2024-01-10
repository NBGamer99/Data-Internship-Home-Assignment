from datetime import timedelta, datetime

from airflow.exceptions import AirflowNotFoundException
from airflow.decorators import dag, task

from utils.DATAProcessing import extract_csv, load_to_db, transform_extracted_data
from utils.DBManager import DatabaseManager
from utils.DBQueries import TABLES_CREATION_QUERY


@task()
def extract():
    """ This function is responsible for extracting data from the source.
    It reads data from 'jobs.csv' and saves the 'context' column data
    to 'staging/extracted' as a text file."""

    try:
        extract_csv("./source/jobs.csv")
    except Exception as e:
        raise AirflowNotFoundException(f"Error while extracting data: {e}")


@task()
def transform():
    """This function is responsible for transforming the extracted data.
    It reads the extracted text files from 'staging/extracted' as JSON,
    cleans the job description, transforms the schema, and saves each item
    to 'staging/transformed' as a JSON file."""

    transform_extracted_data()


@task()
def load():
    """This function is responsible for loading the transformed data into the database.
    It reads the transformed data from 'staging/transformed' and saves it to the SQLite database."""
    load_to_db()



DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """    This function defines the ETL pipeline DAG.

    The DAG consists of three tasks: extract, transform, and load.
    The extract task reads data from 'jobs.csv' and saves the 'context' column data
    to 'staging/extracted' as a text file.
    The transform task reads the extracted text files from 'staging/extracted' as JSON,
    cleans the job description, transforms the schema, and saves each item
    to 'staging/transformed' as a JSON file.
    The load task reads the transformed data from 'staging/transformed' and saves it to the SQLite database."""

    database_manager = DatabaseManager(TABLES_CREATION_QUERY)
    create_tables = database_manager.create_tables()

    create_tables >> extract() >> transform() >> load()

etl_dag()
