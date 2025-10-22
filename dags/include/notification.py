import traceback
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance

def send_email_failure_alert(context):
    # Sends an email alert on DAG failure including the error message
    try:
        task_instance: TaskInstance = context.get("task_instance")
        dag_run: DagRun = context.get("dag_run")
        exception = context.get("exception")
        
        # Get the exception as a string to display in the email
        exception_string = ""
        if exception:
            exception_string = f"<pre>{traceback.format_exc()}</pre>"
            
        subject = f"Airflow DAG Failure: {dag_run.dag_id}"
        html_content = (
            f"<h3>Airflow DAG Failure</h3>"
            f"<b>DAG:</b> {dag_run.dag_id}<br>"
            f"<b>Task:</b> {task_instance.task_id}<br>"
            f"<b>Run ID:</b> {dag_run.run_id}<br>"
            f"<b>Execution Date:</b> {dag_run.execution_date}<br>"
            f"<b>Log URL:</b> <a href='{task_instance.log_url}'>{task_instance.log_url}</a><br><br>"
            f"<b>Error Details:</b><br>"
            f"{exception_string}"
        )

        send_smtp_notification(
            to=["ojokayode13@gmail.com"],
            subject=subject,
            html_content=html_content,
            smtp_conn_id="smtp_conn" 
        )
    except Exception as e:
        print(f"Failed to send notification: {e}")

