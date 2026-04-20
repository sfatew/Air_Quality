from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "chien_bk",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="himawari_dag",
    default_args=default_args,
    description="Download Himawari data",
    schedule="0 0 * * *",
    catchup=False,
    tags=["extract"]
) as dag:

    run_himawari_l2 = BashOperator(
        task_id="run_himawari_l2",
        bash_command="""
        python /opt/airflow/dags/Himawari/download_nc/download_himawari.py --level L2 --years 2023
        """
    )

    run_himawari_l3 = BashOperator(
        task_id="run_himawari_l3",
        bash_command="""
        python /opt/airflow/dags/Himawari/download_nc/download_himawari.py --level L3 --years 2023
        """
    )

    extract_l2_aod = BashOperator(
        task_id="extract_l2_aod",
        bash_command="""
        python /opt/airflow/dags/Himawari/extract_aod/extract_stations_aod_spatial.py --level L2 --years 2022 2023 2024 2025 2026
        """
    )

    extract_l3_aod = BashOperator(
        task_id="extract_l3_aod",
        bash_command="""
        python /opt/airflow/dags/Himawari/extract_aod/extract_stations_aod_spatial.py --level L3 --years 2022 2023 2024 2025 2026
        """
    )

    run_himawari_l2 >> extract_l2_aod
    run_himawari_l3 >> extract_l3_aod

