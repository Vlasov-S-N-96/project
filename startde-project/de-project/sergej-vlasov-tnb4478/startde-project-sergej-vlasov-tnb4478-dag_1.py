import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime


K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"

SUBMIT_NAME = "job_submit"


def _build_submit_operator(task_id: str, application_file: str, link_dag):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=link_dag
    )


def _build_sensor(task_id: str, application_name: str, link_dag):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        dag=link_dag
    )


# Создаем DAG
with DAG("startde-project-sergej-vlasov-tnb4478-dag',
         default_args=default_args,
         schedule_interval=None,
         start_date=pendulum.datetime(2025, 03, 03, tz="UTC"),
         tags=["job_submit"],
         catchup=False) as dag:

    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)


    # Определяем задачу SparkKubernetesOperator

    submit_task = _build_submit_operator(
        task_id=SUBMIT_NAME,
        application_file='items_spark_submit.yaml',
        link_dag=dag
    )

    sensor_task = _build_sensor(
        task_id='sensor',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME}')['metadata']['name']}}}}",
        link_dag=dag
    )

    start >> submit_task >> sensor_task >> end
