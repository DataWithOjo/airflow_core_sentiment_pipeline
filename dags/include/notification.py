import traceback
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance

def send_email_failure_alert(context):
    # Callback function that sends an email alert when an Airflow task fails.

    try:
        task_instance: TaskInstance = context.get("task_instance")
        dag_run: DagRun = context.get("dag_run")
        exception = context.get("exception")
        
        # Explicitly format the traceback from the 'exception' object if it exists.
        exception_string = "No exception details available in context or traceback could not be retrieved."
        if exception:
            try:
                # Use traceback.format_exception to reliably get the stack trace from the object
                formatted_tb = traceback.format_exception(type(exception), exception, exception.__traceback__)
                exception_string = f"<pre>{''.join(formatted_tb)}</pre>"
            except Exception:
                # Fallback if __traceback__ is lost or not present
                exception_string = f"<pre>Error type: {type(exception).__name__}\nMessage: {str(exception)}</pre>"
            
        subject = f"Airflow DAG Failure: {dag_run.dag_id}"
        html_content = (
            f"<h3>Airflow DAG Failure Alert</h3>"
            f"<b>DAG:</b> {dag_run.dag_id}<br>"
            f"<b>Task:</b> {task_instance.task_id}<br>"
            f"<b>Run ID:</b> {dag_run.run_id}<br>"
            f"<b>Logical Date:</b> {dag_run.logical_date}<br>"
            f"<b>Log URL:</b> <a href='{task_instance.log_url}'>Click to view logs</a><br><br>"
            f"<b>Error Details:</b><br>"
            f"{exception_string}"
        )
        # Instantiate and execute EmailOperator directly
        email_op = EmailOperator(
            task_id="send_failure_alert_email",
            to=["ojokayode13@gmail.com"],
            subject=subject,
            html_content=html_content,
            conn_id="smtp_conn"  # Use the same connection ID as your DAG
        )
        
        # Execute the operator to send the email
        email_op.execute(context=context)
        print(f"Successfully sent failure email for {dag_run.dag_id}")

    except Exception as e:
        print(f"ERROR: Failed to send failure notification email: {e}")
        traceback.print_exc()