import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime


K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"

SUBMIT_NAME = "job_submit"
SENSOR_NAME = "job_sensor"


items_datamart_query = """DROP EXTERNAL TABLE IF EXISTS "sergej-vlasov-tnb4478".seller_items CASCADE;
                    CREATE EXTERNAL TABLE "sergej-vlasov-tnb4478".seller_items (
                    sku_id BIGINT,
                    title TEXT,
                    category TEXT,
                    brand  TEXT,
                    seller TEXT,
                    group_type TEXT,
                    country TEXT,
                    availability_items_count BIGINT,
                    ordered_items_count BIGINT,
                    warehouses_count BIGINT,
                    item_price BIGINT,
                    goods_sold_count BIGINT,
                    item_rate FLOAT8,
                    days_on_sell BIGINT,
                    avg_percent_to_sold BIGINT,
                    returned_items_count INTEGER,
                    potential_revenue BIGINT,
                    total_revenue BIGINT,
                    avg_daily_sales FLOAT8,
                    days_to_sold FLOAT8,
                    item_rate_percent FLOAT8
                    )
                    LOCATION ('pxf://startde-project/sergej-vlasov-tnb4478/seller_items?PROFILE=s3:parquet&SERVER=default')
                    ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';"""

unreliable_sellers_query = """ DROP ViEW IF EXISTS "sergej-vlasov-tnb4478".unreliable_sellers_view;
                    CREATE VIEW "sergej-vlasov-tnb4478".unreliable_sellers_view as
                    SELECT 
                        total.seller AS seller,
                        total.availability_items_count AS total_overload_items_count,
                        total.availability_items_count > total.ordered_items_count AS is_unreliable
                    FROM (
                        SELECT 
                            s.seller AS seller,
                            SUM(s.availability_items_count) AS availability_items_count,
                            SUM(s.ordered_items_count) AS ordered_items_count
                        FROM 
                            "sergej-vlasov-tnb4478".seller_items s
                        WHERE 
                            s.days_on_sell > 100 
                        GROUP BY 
                            s.seller
                    ) AS total 
                    """


item_brands_query = """ DROP ViEW IF EXISTS "sergej-vlasov-tnb4478".item_brands_view;
                    CREATE VIEW "sergej-vlasov-tnb4478".item_brands_view as
                    select brand,
                    group_type,
                    country,
                    sum(potential_revenue) as potential_revenue,
                    SUM(total_revenue) as total_revenue,
                    count(sku_id) as items_count
                    from "sergej-vlasov-tnb4478".seller_items
                    group by brand,
                          group_type,
                          country"""

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

# Аргументы по умолчанию
default_args = {
    "owner": "sergej-vlasov-tnb4478",
}
# Создаем DAG
with DAG("startde-project-sergej-vlasov-tnb4478-dag",
         default_args=default_args,
         schedule_interval=None,
         start_date=pendulum.datetime(2025, 3, 3, tz="UTC"),
         tags=["job_submit"],
         catchup=False) as dag:



    # Определяем задачу SparkKubernetesOperator

    submit_task = _build_submit_operator(
        task_id=SUBMIT_NAME,
        application_file='items_spark_submit.yaml',
        link_dag=dag
    )

    sensor_task = _build_sensor(
        task_id=SENSOR_NAME,
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME}')['metadata']['name']}}}}",
        link_dag=dag
    )

    build_datamart = SQLExecuteQueryOperator(
    task_id="items_datamart",
    conn_id=GREENPLUM_ID,
    sql=items_datamart_query,
    autocommit=True,
    split_statements=True,
    return_last=False,
    )


    unreliable_sellers_view = SQLExecuteQueryOperator(
    task_id="create_unreliable_sellers_report_view",
    conn_id=GREENPLUM_ID,
    sql=unreliable_sellers_query,
    autocommit=True,
    split_statements=True,
    return_last=False,
    )


    item_brands_view = SQLExecuteQueryOperator(
    task_id="create_brands_report_view",
    conn_id=GREENPLUM_ID,
    sql=item_brands_query,
    autocommit=True,
    split_statements=True,
    return_last=False,
    )


    submit_task >> sensor_task >> build_datamart 
    build_datamart >> [item_brands_view,unreliable_sellers_view]