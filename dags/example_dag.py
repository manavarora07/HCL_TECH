# Small Airflow-safe placeholder (imports guarded so file can be present without Airflow).
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime

    def noop():
        print("noop")

    with DAG("loyalty_pipeline_example", start_date=datetime(2025,1,1), schedule_interval="@daily", catchup=False) as dag:
        t = PythonOperator(task_id="noop", python_callable=noop)
except Exception:
    # Not running under Airflow — keep file import-safe.
    pass
