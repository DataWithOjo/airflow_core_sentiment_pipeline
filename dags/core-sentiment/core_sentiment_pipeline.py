from airflow.sdk import DAG, task
from datetime import timedelta
from pendulum import datetime
from include.upload_to_minio import upload_to_minio
from include.filter_and_load import filter_and_upload_filtered_file, load_filtered_to_postgres, get_highest_pageviews

# Configuration
pageview_date = "20251010"
pageview_hour = "160000"
year = pageview_date[:4]
year_month = f"{pageview_date[:4]}-{pageview_date[4:6]}"

# File paths
download_url = (
    f"https://dumps.wikimedia.org/other/pageviews/{year}/{year_month}/pageviews-{pageview_date}-{pageview_hour}.gz"
)
local_dir = "/tmp/pageviews"
gz_file = f"{local_dir}/pageviews-{pageview_date}-{pageview_hour}.gz"
extracted_file = f"{local_dir}/pageviews-{pageview_date}-{pageview_hour}"
bucket_name = "wiki-pageviews"
object_key = f"{pageview_date}/{pageview_hour}.txt"
conn_id = "minio_conn"
postgres_conn_id = "postgres_conn_id"
filtered_key = f"{pageview_date}/{pageview_hour}-filtered.csv"
table_name = "pageviews_companies"

with DAG(
    dag_id="core_sentiment_pipeline",
    start_date=datetime(2025, 10, 10),
    schedule=None,
    catchup=False,
    description="Download, extract, upload, filter, and analyze Wikimedia pageviews",
    tags=["wikimedia", "pageviews"]
) as dag:

    # Download the .gz file
    @task.bash(retries=3, retry_delay=timedelta(minutes=10))
    def download_file():
        return (
            f"mkdir -p {local_dir} && "
            f"wget -nv --timeout=300 {download_url} -O {gz_file} && "
            f"echo 'Downloaded {gz_file}'"
        )

    # Extract the file with gunzip
    @task.bash
    def extract_file():
        return (
            f"gunzip -f {gz_file} && "
            f"echo 'Extracted {gz_file} to {extracted_file}'"
        )

    # Upload the extracted file to MinIO
    @task
    def upload_file():
        upload_to_minio(extracted_file, bucket_name, object_key, conn_id)
        print(f"Uploaded to s3://{bucket_name}/{object_key}")

    # Filter the uploaded file for 5 companies
    @task
    def filter_file():
        filter_and_upload_filtered_file(
            source_key=object_key,
            filtered_key=filtered_key,
            minio_conn_id=conn_id,
            bucket_name=bucket_name
        )

    # Load filtered data to PostgreSQL
    @task
    def load_filtered():
        load_filtered_to_postgres(
            filtered_key=filtered_key,
            table_name=table_name,
            minio_conn_id=conn_id,
            bucket_name=bucket_name,
            postgres_conn_id=postgres_conn_id
        )

    # Analyze top-viewed company
    @task
    def analyze_top_company():
        result = get_highest_pageviews(
            postgres_conn_id=postgres_conn_id,
            table_name=table_name
        )
        return result

    # Define DAG dependencies
    download_file() >> extract_file() >> upload_file() >> filter_file() >> load_filtered() >> analyze_top_company()
