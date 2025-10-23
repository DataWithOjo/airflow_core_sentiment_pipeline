import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_minio(local_path: str, bucket: str, object_name: str, conn_id: str):

    # Uploads a file to a MinIO bucket using Airflow's S3Hook.

    print(f"Uploading {local_path} to s3://{bucket}/{object_name}")

    try:
        s3_hook = S3Hook(aws_conn_id=conn_id)
        s3_client = s3_hook.get_conn()

        # Check if the bucket exists
        existing_buckets = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]
        if bucket not in existing_buckets:
            s3_client.create_bucket(Bucket=bucket)
            print(f"Created new bucket: {bucket}")

        # Upload the file 
        s3_hook.load_file(
            filename=local_path,
            key=object_name,
            bucket_name=bucket,
            replace=True,
        )

        print(f"Successfully uploaded {local_path} → s3://{bucket}/{object_name}")

    except Exception as e:
        print(f"Upload failed for {local_path}: {e}")
        raise

    finally:
        # Clean up the local file regardless of success/failure
        if os.path.exists(local_path):
            os.remove(local_path)
            print(f"Cleaned up local file: {local_path}")
