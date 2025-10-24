from airflow.sdk import DAG, task
from datetime import timedelta
from pendulum import datetime
from airflow.providers.smtp.operators.smtp import EmailOperator
from include.upload_to_minio import upload_to_minio
from include.filter_and_load import filter_and_upload_filtered_file, load_filtered_to_postgres, get_highest_pageviews
from include.notification import send_email_failure_alert

# Configuration
pageview_date = "20251010"
pageview_hour = "160000"
year = pageview_date[:4]
year_month = f"{pageview_date[:4]}-{pageview_date[4:6]}"
conn_id = "minio_conn"
postgres_conn_id = "postgres_conn_id"
bucket_name = "wiki-pageviews"
table_name = "pageviews_companies"

# File paths
download_url = (
    f"https://dumps.wikimedia.org/other/pageviews/{year}/{year_month}/pageviews-{pageview_date}-{pageview_hour}.gz"
)
local_dir = "/tmp/pageviews"
gz_file = f"{local_dir}/pageviews-{pageview_date}-{pageview_hour}.gz"
extracted_file = f"{local_dir}/pageviews-{pageview_date}-{pageview_hour}"
object_key = f"{pageview_date}/{pageview_hour}.txt"
filtered_key = f"{pageview_date}/{pageview_hour}-filtered.csv"

with DAG(
    dag_id="core_sentiment_pipeline",
    start_date=datetime(2025, 10, 10),
    schedule=None,
    catchup=False,
    description="Download, extract, upload, filter, and analyze Wikimedia pageviews",
    tags=["wikimedia", "pageviews"],
    default_args={"on_failure_callback": send_email_failure_alert}
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
        raise ValueError("This is a deliberate test failure")
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
        top_company_data = get_highest_pageviews(
            postgres_conn_id=postgres_conn_id,
            table_name=table_name
        )
        return top_company_data
    
    # Sends notifications
    @task()
    def send_notification_task(top_company_data: dict | None = None, **context):
        # Sends a summary email using the results from the analyze_top_company task.

        execution_date_str = context.get('ds')

        if top_company_data:
            company = top_company_data["company"]
            total_views = top_company_data["total_views"]

            html_content = f"""
                <h3>Wikimedia Pageviews Pipeline Update</h3>
                <p>Hi Kayode,</p>
                <p>Your Airflow pipeline has completed <b>successfully</b>.</p>
                <p>The top-viewed company is <b>{company}</b> with <b>{total_views}</b> total views.</p>
                <p>Table: <b>{table_name}</b> | Bucket: <b>{bucket_name}</b></p>
                <br><p>Kind Regards,<br><b>CDE Airflow Team</b></p>
            """
        else:
            html_content = f"""
                <h3>Wikimedia Pageviews Pipeline Update</h3>
                <p>Hi Kayode,</p>
                <p>Your Airflow pipeline has completed successfully, but no company data was found for this run.</p>
                <p>Table: <b>{table_name}</b> | Bucket: <b>{bucket_name}</b></p>
                <br><p>Kind Regards,<br><b>CDE Airflow Team</b></p>
            """

        email = EmailOperator(
            task_id="send_notification",
            to=["ojokayode13@gmail.com"],
            subject=f"Wikimedia Pageviews Pipeline Completed Successfully — {execution_date_str}",
            html_content=html_content,
            conn_id="smtp_conn",
        )

        # Run the email operator inside the Python task
        email.execute(context=context)
        print("Notification email sent successfully.")
    
    # instantiate the tasks
    download_task = download_file()
    extract_task = extract_file()
    upload_task = upload_file()
    filter_task = filter_file()
    load_task = load_filtered()
    analyze_task = analyze_top_company()
    
    # Pass the output of analyze_task as the argument to send_notification_task
    notification_task = send_notification_task(analyze_task)

    # Set the dependencies using the task instances
    download_task >> extract_task >> upload_task >> filter_task >> load_task >> analyze_task >> notification_task
