"""

# Ifood Data Architect Test - Pipeline

"""

from datetime import datetime, timedelta

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from infrastructure.emr_cluster_settings import EmrSettings

default_args = {
    "owner": "renanfernandos@gmail.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 8, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ifood_ingestion_pipeline_dag",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 3 * * 1-5",
    catchup=False,
)

dag.doc_md = __doc__

import_raw_consuer_main_class = (
    "com.ifood.dataarchitect.etl.layer.raw.ImportRawConsumer"
)
import_raw_order_main_class = "com.ifood.dataarchitect.etl.layer.raw.ImportRawOrder"
import_raw_restaurant_main_class = (
    "com.ifood.dataarchitect.etl.layer.raw.ImportRawRestaurant"
)
import_raw_order_status_main_class = (
    "com.ifood.dataarchitect.etl.layer.raw.ImportRawOrderStatus"
)
process_trusted_order_main_class = (
    "com.ifood.dataarchitect.etl.layer.trusted.ProcessTrustedOrder"
)
process_trusted_order_items_main_class = (
    "com.ifood.dataarchitect.etl.layer.trusted.ProcessTrustedOrderItems"
)
process_trusted_order_status_main_class = (
    "com.ifood.dataarchitect.etl.layer.trusted.ProcessTrustedOrderStatus"
)
spark_app_jar_location = (
    "s3n://ifood-data-architect-resources-renan/ifood-data-architect-test-1.0.0.jar"
)

# These dataset settings can be replaceable!
# I used these ones to my tests, feel free to change...
source_datasets_location = "s3n://ifood-data-architect-test-source"
target_raw_datasets_location = "s3n://ifood-data-architect-raw-layer-renan"
target_trusted_datasets_location = "s3n://ifood-data-architect-trusted-layer-renan"


def create_emr_job_flow(**kwargs):
    # Required params to be changed:
    # - aws_account_id
    # - aws_region
    # - ec2_key_pair
    # - ec2_subnet_id

    emr_settings = EmrSettings(
        aws_account_id="????????????",
        aws_region="us-east-1",
        ec2_key_pair="???????????",
        ec2_subnet_id="????????????",
        cluster_name=f"Ifood Data Architect Test | {kwargs['ds']}",
        master_instance_type="m5.4xlarge",
        master_instance_count=1,
        core_instance_type="m5.4xlarge",
        core_instance_count=1,
        core_instance_market="ON_DEMAND",
        task_instance_type="c5.2xlarge",
        task_instance_count=1,
        task_instance_market="SPOT",
        step_concurrency_level=4
    )

    job_flow_id = EmrCreateJobFlowOperator(
        task_id="create_cluster_emr_job_task",
        aws_conn_id="aws_default",
        region_name=emr_settings.aws_region,
        job_flow_overrides=emr_settings.crete_job_flow_overrides(),
        dag=dag,
    ).execute(kwargs)

    kwargs["ti"].xcom_push(key="job_flow_id", value=job_flow_id)


def add_import_raw_dataset_step_job(**kwargs):
    steps = [
        {
            "Name": kwargs.get("step_name"),
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--num-executors",
                    kwargs.get("num_executors", "1"),
                    "--executor-cores",
                    kwargs.get("executor_cores", "4"),
                    "--driver-memory",
                    kwargs.get("driver_memory", "4g"),
                    "--executor-memory",
                    kwargs.get("executor_memory", "8g"),
                    "--class",
                    kwargs.get("main_class"),
                    spark_app_jar_location,
                    kwargs.get("source_path"),
                    kwargs.get("source_format"),
                    kwargs.get("target_path"),
                    kwargs.get("target_format"),
                ],
            },
        }
    ]
    job_flow_id = kwargs["ti"].xcom_pull(
        task_ids="create_cluster_emr_job", key="job_flow_id"
    )

    step_ids = EmrAddStepsOperator(
        task_id=kwargs.get("task_id"),
        aws_conn_id="aws_default",
        job_flow_id=job_flow_id,
        steps=steps,
        dag=dag,
    ).execute(kwargs)

    kwargs["ti"].xcom_push(key=kwargs.get("step_id_key"), value=step_ids[0])


def add_process_trusted_dataset_step_job(**kwargs):
    steps = [
        {
            "Name": kwargs.get("step_name"),
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--num-executors",
                    kwargs.get("num_executors", "1"),
                    "--executor-cores",
                    kwargs.get("executor_cores", "4"),
                    "--driver-memory",
                    kwargs.get("driver_memory", "4g"),
                    "--executor-memory",
                    kwargs.get("executor_memory", "8g"),
                    "--class",
                    kwargs.get("main_class"),
                    spark_app_jar_location,
                    kwargs.get("target_path"),
                    kwargs.get("target_format"),
                ],
            },
        }
    ]

    job_flow_id = kwargs["ti"].xcom_pull(
        task_ids="create_cluster_emr_job", key="job_flow_id"
    )

    step_ids = EmrAddStepsOperator(
        task_id=kwargs.get("task_id"),
        aws_conn_id="aws_default",
        job_flow_id=job_flow_id,
        steps=steps,
        dag=dag,
    ).execute(kwargs)

    kwargs["ti"].xcom_push(key=kwargs.get("step_id_key"), value=step_ids[0])


create_cluster_emr = PythonOperator(
    task_id="create_cluster_emr_job",
    depends_on_past=False,
    python_callable=create_emr_job_flow,
    provide_context=True,
    dag=dag,
)

create_import_raw_consumer_dataset_step_job = PythonOperator(
    task_id="create_import_raw_consumer_dataset_step_job",
    depends_on_past=False,
    python_callable=add_import_raw_dataset_step_job,
    op_kwargs={
        "task_id": "create_import_raw_consumer_dataset_step_job",
        "step_name": "Import raw consumer dataset",
        "step_id_key": "import_raw_consumer_dataset_step",
        "main_class": import_raw_consuer_main_class,
        "source_path": f"{source_datasets_location}/consumer.csv.gz",
        "source_format": "csv",
        "target_path": target_raw_datasets_location,
        "target_format": "parquet",
    },
    provide_context=True,
    dag=dag,
)

create_import_raw_order_dataset_step_job = PythonOperator(
    task_id="create_import_raw_order_dataset_step_job",
    depends_on_past=False,
    python_callable=add_import_raw_dataset_step_job,
    op_kwargs={
        "task_id": "create_import_raw_order_dataset_step_job",
        "step_name": "Import raw order dataset",
        "step_id_key": "import_raw_order_dataset_step",
        "main_class": import_raw_order_main_class,
        "num_executors": "2",
        "driver_memory": "8g",
        "executor_memory": "8g",
        "source_path": f"{source_datasets_location}/order.json.gz",
        "source_format": "json",
        "target_path": target_raw_datasets_location,
        "target_format": "parquet",
    },
    provide_context=True,
    dag=dag,
)

create_import_raw_restaurant_dataset_step_job = PythonOperator(
    task_id="create_import_raw_restaurant_dataset_step_job",
    depends_on_past=False,
    python_callable=add_import_raw_dataset_step_job,
    op_kwargs={
        "task_id": "create_import_raw_restaurant_dataset_step_job",
        "step_name": "Import raw restaurant dataset",
        "step_id_key": "import_raw_restaurant_dataset_step",
        "main_class": import_raw_restaurant_main_class,
        "source_path": f"{source_datasets_location}/restaurant.csv.gz",
        "source_format": "csv",
        "target_path": target_raw_datasets_location,
        "target_format": "parquet",
    },
    provide_context=True,
    dag=dag,
)

create_import_raw_order_status_dataset_step_job = PythonOperator(
    task_id="create_import_raw_order_status_dataset_step_job",
    depends_on_past=False,
    python_callable=add_import_raw_dataset_step_job,
    op_kwargs={
        "task_id": "create_import_raw_order_status_dataset_step_job",
        "step_name": "Import raw order status dataset",
        "step_id_key": "import_raw_order_status_dataset_step",
        "main_class": import_raw_order_status_main_class,
        "source_path": f"{source_datasets_location}/status.json.gz",
        "source_format": "json",
        "target_path": target_raw_datasets_location,
        "target_format": "parquet",
    },
    provide_context=True,
    dag=dag,
)

sensor_import_raw_consumer_step = EmrStepSensor(
    task_id="sensor_import_raw_consumer_step",
    job_flow_id=(
        """{{ task_instance.xcom_pull(task_ids='create_cluster_emr_job',
                                      key='job_flow_id') }}"""
    ),
    step_id=(
        """{{ task_instance.xcom_pull(task_ids='create_import_raw_consumer_dataset_step_job',
                                      key='import_raw_consumer_dataset_step') }}"""
    ),
    dag=dag,
)

sensor_import_raw_order_step = EmrStepSensor(
    task_id="sensor_import_raw_order_step",
    job_flow_id=(
        "{{ task_instance.xcom_pull(task_ids='create_cluster_emr_job', key='job_flow_id') }}"
    ),
    step_id=(
        """{{ task_instance.xcom_pull(task_ids='create_import_raw_order_dataset_step_job',
                                      key='import_raw_order_dataset_step') }}"""
    ),
    dag=dag,
)

sensor_import_raw_restaurant_step = EmrStepSensor(
    task_id="sensor_import_raw_restaurant_step",
    job_flow_id=(
        "{{ task_instance.xcom_pull(task_ids='create_cluster_emr_job', key='job_flow_id') }}"
    ),
    step_id=(
        """{{ task_instance.xcom_pull(task_ids='create_import_raw_restaurant_dataset_step_job',
                                      key='import_raw_restaurant_dataset_step') }}"""
    ),
    dag=dag,
)

sensor_import_raw_order_status_step = EmrStepSensor(
    task_id="sensor_import_raw_order_status_step",
    job_flow_id=(
        "{{ task_instance.xcom_pull(task_ids='create_cluster_emr_job', key='job_flow_id') }}"
    ),
    step_id=(
        """{{ task_instance.xcom_pull(task_ids='create_import_raw_order_status_dataset_step_job',
                                      key='import_raw_order_status_dataset_step') }}"""
    ),
    dag=dag,
)

waiting_import_raw_datasets_steps = DummyOperator(
    task_id="waiting_import_raw_datasets_steps", dag=dag
)

create_process_trusted_order_dataset_step_job = PythonOperator(
    task_id="create_process_trusted_order_dataset_step_job",
    depends_on_past=False,
    python_callable=add_process_trusted_dataset_step_job,
    op_kwargs={
        "task_id": "create_process_trusted_order_dataset_step_job",
        "step_name": "Process trusted order dataset",
        "step_id_key": "process_trusted_order_dataset_step",
        "num_executors": "2",
        "driver_memory": "8g",
        "executor_memory": "8g",
        "main_class": process_trusted_order_main_class,
        "target_path": target_trusted_datasets_location,
        "target_format": "parquet",
    },
    provide_context=True,
    dag=dag,
)

create_process_trusted_order_items_dataset_step_job = PythonOperator(
    task_id="create_process_trusted_order_items_dataset_step_job",
    depends_on_past=False,
    python_callable=add_process_trusted_dataset_step_job,
    op_kwargs={
        "task_id": "create_process_trusted_order_items_dataset_step_job",
        "step_name": "Process trusted order items dataset",
        "step_id_key": "process_trusted_order_items_dataset_step",
        "num_executors": "2",
        "driver_memory": "8g",
        "executor_memory": "8g",
        "main_class": process_trusted_order_items_main_class,
        "target_path": target_trusted_datasets_location,
        "target_format": "parquet",
    },
    provide_context=True,
    dag=dag,
)

create_process_trusted_order_status_dataset_step_job = PythonOperator(
    task_id="create_process_trusted_order_status_dataset_step_job",
    depends_on_past=False,
    python_callable=add_process_trusted_dataset_step_job,
    op_kwargs={
        "task_id": "create_process_trusted_order_status_dataset_step_job",
        "step_name": "Process trusted order status dataset",
        "step_id_key": "process_trusted_order_status_dataset_step",
        "num_executors": "2",
        "driver_memory": "8g",
        "executor_memory": "8g",
        "main_class": process_trusted_order_status_main_class,
        "target_path": target_trusted_datasets_location,
        "target_format": "parquet",
    },
    provide_context=True,
    dag=dag,
)

sensor_process_trusted_order_step = EmrStepSensor(
    task_id="sensor_process_trusted_order_step",
    job_flow_id=(
        "{{ task_instance.xcom_pull(task_ids='create_cluster_emr_job', key='job_flow_id') }}"
    ),
    step_id=(
        """{{ task_instance.xcom_pull(task_ids='create_process_trusted_order_dataset_step_job',
                                      key='process_trusted_order_dataset_step') }}"""
    ),
    dag=dag,
)

sensor_process_trusted_order_items_step = EmrStepSensor(
    task_id="sensor_process_trusted_order_items_step",
    job_flow_id=(
        "{{ task_instance.xcom_pull(task_ids='create_cluster_emr_job', key='job_flow_id') }}"
    ),
    step_id=(
        """{{ task_instance.xcom_pull(task_ids='create_process_trusted_order_items_dataset_step_job',
                                      key='process_trusted_order_items_dataset_step') }}"""
    ),
    dag=dag,
)

sensor_process_trusted_order_status_step = EmrStepSensor(
    task_id="sensor_process_trusted_order_status_step",
    job_flow_id=(
        "{{ task_instance.xcom_pull(task_ids='create_cluster_emr_job', key='job_flow_id') }}"
    ),
    step_id=(
        """{{ task_instance.xcom_pull(task_ids='create_process_trusted_order_status_dataset_step_job',
                                      key='process_trusted_order_status_dataset_step') }}"""
    ),
    dag=dag,
)

waiting_process_trusted_datasets_steps = DummyOperator(
    task_id="waiting_process_trusted_datasets_steps", dag=dag
)

terminate_cluster_emr_job = EmrTerminateJobFlowOperator(
    task_id="terminate_cluster_emr_job",
    aws_conn_id="aws_default",
    job_flow_id=(
        "{{ task_instance.xcom_pull(task_ids='create_cluster_emr_job', key='job_flow_id') }}"
    ),
    dag=dag,
)

create_cluster_emr >> [
    create_import_raw_consumer_dataset_step_job,
    create_import_raw_order_dataset_step_job,
    create_import_raw_restaurant_dataset_step_job,
    create_import_raw_order_status_dataset_step_job,
]
create_import_raw_consumer_dataset_step_job >> sensor_import_raw_consumer_step,
create_import_raw_order_dataset_step_job >> sensor_import_raw_order_step,
create_import_raw_restaurant_dataset_step_job >> sensor_import_raw_restaurant_step,
create_import_raw_order_status_dataset_step_job >> sensor_import_raw_order_status_step
[
    sensor_import_raw_consumer_step,
    sensor_import_raw_order_step,
    sensor_import_raw_restaurant_step,
    sensor_import_raw_order_status_step,
] >> waiting_import_raw_datasets_steps
waiting_import_raw_datasets_steps >> [
    create_process_trusted_order_dataset_step_job,
    create_process_trusted_order_items_dataset_step_job,
    create_process_trusted_order_status_dataset_step_job,
]
create_process_trusted_order_dataset_step_job >> sensor_process_trusted_order_step
create_process_trusted_order_items_dataset_step_job >> sensor_process_trusted_order_items_step
create_process_trusted_order_status_dataset_step_job >> sensor_process_trusted_order_status_step
[
    sensor_process_trusted_order_step,
    sensor_process_trusted_order_items_step,
    sensor_process_trusted_order_status_step,
] >> waiting_process_trusted_datasets_steps
waiting_process_trusted_datasets_steps >> terminate_cluster_emr_job
