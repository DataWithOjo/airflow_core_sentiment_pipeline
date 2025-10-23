import io
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv 


def filter_and_upload_filtered_file(
    source_key: str, filtered_key: str, minio_conn_id: str, bucket_name: str
):
    print("Filtering data for companies: Amazon, Apple, Facebook, Google, Microsoft")
    s3_hook = S3Hook(aws_conn_id=minio_conn_id)

    # Download file from MinIO
    obj_content = s3_hook.read_key(key=source_key, bucket_name=bucket_name)

    # Load into pandas
    df = pd.read_csv(
        io.StringIO(obj_content),
        sep=r"\s+",
        names=["project", "page_title", "views", "bytes"],
        engine="python"
    )

    # Filter for the 5 companies
    companies = ["Amazon", "Apple", "Facebook", "Google", "Microsoft"]
    df_filtered = df[df["page_title"].str.contains("|".join(companies), case=False, na=False)]
    print(f"Filtered down to {len(df_filtered)} rows containing the 5 companies")

    # Data cleaning
    df_filtered["project"] = df_filtered["project"].replace('', None)
    df_filtered["views"] = df_filtered["views"].astype(int)
    df_filtered["bytes"] = df_filtered["bytes"].astype(int)

    # Save filtered data back to MinIO
    csv_buffer = io.StringIO()

    # Write with utf-8, include headers, and quote non-numeric fields
    df_filtered.to_csv(
        csv_buffer,
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_NONNUMERIC
    )

    # Upload to MinIO
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=filtered_key,
        bucket_name=bucket_name,
        replace=True
    )

    print(f"Uploaded filtered file to s3://{bucket_name}/{filtered_key}")


def load_filtered_to_postgres(
    filtered_key: str, table_name: str, minio_conn_id: str, bucket_name: str, postgres_conn_id: str
):

    # Loads the filtered CSV file from MinIO into a PostgreSQL table.

    s3_hook = S3Hook(aws_conn_id=minio_conn_id)
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = pg_hook.get_sqlalchemy_engine()

    obj_content = s3_hook.read_key(key=filtered_key, bucket_name=bucket_name)
    df = pd.read_csv(io.StringIO(obj_content))

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Loaded {len(df)} rows into table {table_name}")


def get_highest_pageviews(postgres_conn_id: str, table_name: str):

    # Performs a simple analysis to find the company with the highest pageviews.

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    sql_query = f"""
    SELECT
        page_title,
        SUM(views) as total_views
    FROM
        {table_name}
    GROUP BY
        page_title
    ORDER BY
        total_views DESC
    LIMIT 1;
    """
    result = pg_hook.get_first(sql_query)
    if result:
        company, total_views = result
        print(f"The company with the highest pageviews is {company} with {total_views} views.")
        return {"company": company, "total_views": total_views}
    else:
        print("No data found to analyze.")
        return None