# Витрина Данных для Аналитиков KarpovZone

## Цель проекта

Предоставить аналитикам агрегированные метрики по карточкам товаром, популярным категориям и прочим параметрам на маркетплейсе KarpovZone. Цель также заключается в демонстрации совместной работы DWH (Greenplum), Data Lake (Spark) и Data Orchestrator (Airflow).

## Цель создания витрины данных
Создание инструментов для упрощения аналитики данных по товарам представленным на площадке KarpovZone.

## Архитектура решения

1.  **Data Lake (Spark):**
    *   Чтение исходных данных в формате Parquet из S3 бакета `startde-raw` (VK.Cloud).
    *   Выполнение преобразований и агрегаций данных с использованием PySpark.
    *   Сохранение агрегированных данных в промежуточный Parquet формат для последующей загрузки в DWH.
2.  **Data Orchestration (Airflow):**
    *   Определение DAG (Directed Acyclic Graph) в Airflow для оркестрации задач.
    *   Запуск Spark-приложения для обработки данных.
    *   Выполнение SQL-запросов для создания витрины данных в Greenplum.
3.  **Data Warehouse (Greenplum):**
    *   Создание таблиц витрины данных в Greenplum.
    *   Загрузка агрегированных данных из промежуточного Parquet формата.
    *   Предоставление аналитикам доступа к витрине данных для выполнения запросов и построения отчетов.

## Образовательные результаты проекта

*   Использование Airflow, Spark и Greenplum в связке для построения пайплайнов обработки данных.
*   Понимание принципов работы с озером данных и хранилищем данных.
*   Приобретение навыков построения отчётов DE по ТЗ от заказчика.

## Технологии

*   **Python:**  Для разработки Spark-приложения и Airflow DAG.
    *   **Pandas:**  Для локальной обработки данных (например, при тестировании).
    *   **PySpark:**  Для обработки больших объемов данных в Spark.
*   **SQL:**  Для работы с Greenplum и запросов к данным.
*   **Apache Spark:**  Для обработки и агрегации данных в Data Lake.
*   **Apache Airflow:**  Для оркестрации ETL-процессов.
*   **Greenplum:**  В качестве хранилища данных (DWH).
*   **AWS S3 :**  Для хранения исходных и промежуточных данных.
*   **Git/GitHub:** 

## Исходные данные

Исходные данные размещены в S3 бакете `startde-raw` (VK.Cloud) в формате Parquet. Данные содержат информацию о товарах на маркетплейсе KarpovZone.
<details>
  <summary>Посмотреть исходные данные</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/Amazon_s3/Amazon_s3.jpg" alt="startde-raw/raw_items">
</details>

<table style="border-collapse: collapse;">
  <tr>
    <td style="padding: 0; text-align: center; border: none;">
      <a href="https://github.com/Vlasov-S-N-96/project/tree/main/startde-raw/raw_items">
        <img src="https://raw.githubusercontent.com/Vlasov-S-N-96/project/main/icons/document.svg"
             alt="Нажми..."
             style="width: 50px; height: auto; transition: transform 0.2s ease-in-out; cursor: pointer;"
             onmouseover="this.style.transform='scale(1.1)';"
             onmouseout="this.style.transform='scale(1)';"
             title="Перейти к документу" />
      </a>
    </td>
    <td style="text-align: left; vertical-align: middle; padding-left: 10px; border: none;">
      raw_items
    </td>
  </tr>
</table>



### Описание полей

| Поле                       | Тип      | Описание                                                                                                                                     |
| -------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `sku_id`                   | `bigint` | Уникальный идентификатор товара (Stock Keeping Unit).                                                                                      |
| `title`                    | `string` | Название товара, указанное продавцом.                                                                                                       |
| `category`                 | `string` | Категория товара (например, "Электроника", "Одежда", "Дом и сад").                                                                            |
| `brand`                    | `string` | Название бренда, производящего товар.                                                                                                       |
| `seller`                   | `string` | Название или идентификатор продавца.                                                                                                         |
| `group_type`               | `string` | Группа товара (например, "Смартфоны", "Кроссовки", "Кухонная техника").                                                                      |
| `country`                  | `string` | Страна производства товара.                                                                                                                |
| `availability_items_count` | `bigint` | Текущее количество товара в наличии на складах.                                                                                               |
| `ordered_items_count`      | `bigint` | Общее количество товара, которое было заказано.                                                                                              |
| `warehouses_count`         | `bigint` | Количество складов, на которых размещен товар.                                                                                                |
| `item_price`               | `bigint` | Текущая цена товара.                                                                                                                      |
| `goods_sold_count`         | `bigint` | Количество успешно проданных единиц товара.                                                                                                  |
| `item_rate`                | `double` | Рейтинг товара (например, 4.5 из 5).                                                                                                     |
| `days_on_sell`             | `bigint` | Количество дней с момента размещения товара на платформе.                                                                                    |
| `avg_percent_to_sold`      | `bigint` | Средний процент выкупа товара (отношение количества проданных единиц к общему количеству заказов).                                            |

## Задачи

1.  **Настройка окружения:**

    *   Установка и настройка необходимых инструментов:
        *   Apache Airflow
        *   Apache Spark
        *   Greenplum
        *   Python (с библиотеками pandas, PySpark)
    *   Получение доступа к S3 хранилищу `startde-raw` (VK.Cloud).
    *   Настройка подключения к Greenplum хранилищу.
2.  **Разработка Spark Job :**

    *   Чтение данных о товарах из S3 бакета `startde-raw/raw_items` в формате Parquet.
    *   Обогащение данных дополнительными параметрами и агрегатами:
        *   `returned_items_count` (количество товаров на которое оформлен возврат).
        *   `potential_revenue` (потенциальный доход от остатков товаров).
        *   `total_revenue` (доход от выполненных заказов с учетом возвратов).
        *   `avg_daily_sales` (среднее количество продаж с момента запуска).
        *   `days_to_sold` (количество дней которое потребуется для продажи всех доступных остатков товара).
        *   `item_rate_percent` (относительный ранг рейтинга товара).
    *   Сохранение результата в формате Parquet в S3 бакет `startde-project/{USER_LOGIN}/seller_items`.
3.  **Разработка Airflow DAG :**

    *   Создание DAG, который выполняет следующие задачи:
        *   Запуск Spark Job (используя `SparkKubernetesOperator`).
        *   Создание внешней таблицы `seller_items` в Greenplum (используя `SQLExecuteQueryOperator` и Greenplum PXF).
        *   Создание View-таблицы `unreliable_sellers_view` в Greenplum, содержащей список ненадежных продавцов.
        *   Создание View-таблицы `item_brands_view` в Greenplum, содержащей отчет по брендам.
    *   Настройка зависимостей между задачами DAG.
    *   Обеспечение идемпотентности операций создания таблиц (использование `IF NOT EXISTS`).
4.  **Разработка SQL-скриптов для Greenplum:**

    *   Создание SQL-скриптов для создания внешней таблицы `seller_items` на основе данных в S3.
    *   Создание SQL-скриптов для создания View-таблицы `unreliable_sellers_view` (на основе таблицы `seller_items`).
    *   Создание SQL-скриптов для создания View-таблицы `item_brands_view` (на основе таблицы `seller_items`).
5.  **Тестирование и проверка:**

    *   Проверка успешного выполнения Airflow DAG.
    *   Проверка создания таблиц и View в Greenplum.
    *   Проверка целостности и корректности данных в Greenplum.

<style>
.styled-link {
  color: blue; /* Цвет ссылки по умолчанию */
  text-decoration: none; /* Убрать подчеркивание по умолчанию */
}

.styled-link:hover {
  color: orange; /* Цвет ссылки при наведении */
  text-decoration: underline; /* Добавить подчеркивание при наведении */
}
</style>

## Техническая документация

<a href="https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html#airflow-providers-cncf-kubernetes-operators-sparkkubernetes" class="styled-link">
airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator</a> - для отправки Spark задач на кластер

<a href="https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/sensors.html#airflow-providers-cncf-kubernetes-sensors-sparkkubernetes" class="styled-link">
airflow.providers.cncf.kubernetes.sensors.spark_kubernetes.SparkKubernetesSensor</a> - для отслеживания статуса Spark задач

<a href="https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html#airflow-providers-common-sql-operators-sql-sqlexecutequeryoperator" class="styled-link">
airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator</a> - для выполнения запросов к Greenplum

## Итоговый формат таблиц

### Таблица: seller_items

| Поле                       | Тип (GP)       | Описание                                                                                                 |
| -------------------------- | -------------- | -------------------------------------------------------------------------------------------------------- |
| `sku_id`                   | `BIGINT`       | Уникальный идентификатор товара (Stock Keeping Unit).                                                  |
| `title`                    | `TEXT`         | Название товара, указанное продавцом.                                                                   |
| `category`                 | `TEXT`         | Категория товара, к которой он относится.                                                                |
| `brand`                    | `TEXT`         | Название бренда, производящего товар.                                                                   |
| `seller`                   | `TEXT`         | Название или идентификатор продавца.                                                                     |
| `group_type`               | `TEXT`         | Группа товара, к которой он относится.                                                                 |
| `country`                  | `TEXT`         | Страна производства товара.                                                                            |
| `availability_items_count` | `BIGINT`       | Текущее количество товара в наличии на складах.                                                           |
| `ordered_items_count`      | `BIGINT`       | Общее количество товара, которое было заказано.                                                           |
| `warehouses_count`         | `BIGINT`       | Количество складов, на которых размещен товар.                                                            |
| `item_price`               | `BIGINT`       | Текущая цена товара.                                                                                     |
| `goods_sold_count`         | `BIGINT`       | Количество проданных единиц товара за определенный период.                                                  |
| `item_rate`                | `FLOAT8`       | Рейтинг товара, рассчитанный на основе отзывов пользователей.                                                    |
| `days_on_sell`             | `BIGINT`       | Количество дней, прошедших с момента размещения товара на платформе.                                                    |
| `avg_percent_to_sold`      | `BIGINT`       | Средний процент выкупа товара.                                                                             |
| `returned_items_count`     | `INTEGER`      | Количество товаров на которое оформлен возврат.                                                              |
| `potential_revenue`        | `BIGINT`       | Потенциальный доход от остатков товаров.                                                                   |
| `total_revenue`            | `BIGINT`       | Доход от выполненных заказов с учетом возвратов.                                                            |
| `avg_daily_sales`          | `FLOAT8`       | Среднее количество продаж с момента запуска.                                                               |
| `days_to_sold`             | `FLOAT8`       | Количество дней, которое потребуется для продажи всех доступных остатков товара.                               |
| `item_rate_percent`        | `FLOAT8`       | Относительный ранг (т. е. процентиль).                                                                     |

### View: unreliable_sellers_view

| Поле                          | Тип      | Описание                                                                                                                                                                                              |
| ----------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `seller`                      | `TEXT`   | Информация о продавце.                                                                                                                                                                               |
| `total_overload_items_count`  | `BIGINT` | Количество товаров на складах (для ненадежных продавцов).                                                                                                                                             |
| `is_unreliable`               | `BOOLEAN`| Признак НЕ надежности.                                                                                                                                                                                  |

### View: item_brands_view

| Поле               | Тип (GP) | Описание                                            |
| ------------------ | -------- | --------------------------------------------------- |
| `brand`            | `TEXT`   | Имя бренда.                                        |
| `group_type`       | `TEXT`   | Группа товаров.                                   |
| `country`          | `TEXT`   | Страна производства.                                |
| `potential_revenue`| `FLOAT8` | Суммарный итоговый потенциальный доход.            |
| `total_revenue`    | `FLOAT8` | Суммарный итоговый доход.                           |
| `items_count`      | `BIGINT` | Количество позиций бренда (количество товаров).      |

## Результат

В результате выполнения проекта будет создана витрина данных в Greenplum, содержащая агрегированные метрики по товарам, брендам, категориям и другим параметрам, полезным для анализа эффективности и популярности товаров на маркетплейсе KarpovZone. Аналитики смогут использовать эту витрину для создания отчетов и дашбордов, позволяющих принимать обоснованные решения на основе данных.




## Разработка Spark Job

```python
import io
import sys
import uuid
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql import types

USER = "sergej-vlasov-tnb4478"  # Замените на ваш логин
DATA_PATH = "s3a://startde-raw/raw_items"  # Путь к исходным данным в S3
TARGET_PATH = f"s3a://startde-project/{USER}/seller_items"  # Путь для сохранения результата в S3

def _spark_session():
    """
    Создает и возвращает SparkSession с необходимой конфигурацией для работы с S3.
    """
    return (SparkSession.builder
            .appName("SparkJob-items-" + uuid.uuid4().hex)  # Уникальное имя приложения
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")  # Подключаем AWS SDK для работы с S3
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")  # Endpoint для VK Cloud
            .config('spark.hadoop.fs.s3a.region', "ru-msk")  # Регион VK Cloud
            .config('spark.hadoop.fs.s3a.access.key', "XXX")  # Access Key для S3
            .config('spark.hadoop.fs.s3a.secret.key', "XXXXXX")  # Secret Key для S3
            .getOrCreate()) 

def main():
    """
    Основная функция Spark Job, выполняет чтение, преобразование и запись данных.
    """
    spark = _spark_session()  # Создаем SparkSession
    item_df = spark.read.parquet(DATA_PATH)  # Читаем данные из S3 в DataFrame
```

## 1. Количество товаров на которое оформлен возврат 

```python
    item_df = item_df.withColumn("returned_items_count",
                                  F.round((F.col("ordered_items_count") * (1 - (F.col("avg_percent_to_sold") / 100))), 0).cast("int")) 
```
<details>
  <summary>Показать расчет returned_items_count</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/py_spark_seller_items/py_spark_raw_items_1.jpg" alt="Расчет returned_items_count">
</details>

## 2. Потенциальный доход от остатков товаров и товаров в заказе

```python 
                     .withColumn("potential_revenue",
                                 ((F.col("availability_items_count") + (F.col("ordered_items_count")) * F.col("item_price")).cast("bigint")))
``` 
<details>
  <summary>Показать расчет potential_revenue</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/py_spark_seller_items/py_spark_raw_items_2.jpg" alt="Расчет potential_revenue">
</details>

## 3. Доход от выполненных заказов с учетом возвратов

```python                     
                     .withColumn("total_revenue",
                                 ((F.col("ordered_items_count") - F.col("returned_items_count") + F.col("goods_sold_count")) * F.col("item_price")).cast("bigint"))
```
<details>
  <summary>Показать расчет total_revenue</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/py_spark_seller_items/py_spark_raw_items_3.jpg" alt="Расчет total_revenue">
</details>

## 4. Среднее количество продаж с момента запуска 

```python   
                     .withColumn("avg_daily_sales",
                                 F.when(F.col("days_on_sell") > 0, (F.col("goods_sold_count") / F.col("days_on_sell"))).otherwise(0).cast("double")) \
```
<details>
  <summary>Показать расчет avg_daily_sales</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/py_spark_seller_items/py_spark_raw_items_4.jpg" alt="Расчет avg_daily_sales">
</details>

## 5. Количество дней которое потребуется для продажи всех доступных остатков товара 

```python 
                     .withColumn("days_to_sold",
                                 F.when(F.col("avg_daily_sales") > 0, (F.col("availability_items_count") / F.col("avg_daily_sales"))).otherwise(F.lit(None)).cast("double"))
```
<details>
  <summary>Показать расчет days_to_sold</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/py_spark_seller_items/py_spark_raw_items_5.jpg" alt="Расчет days_to_sold">
</details>

## 6. Расчет percent_rank относительно рейтинга товара 

```python 
    # Создаем оконную функцию для расчета percent_rank
    w = Window.orderBy("item_rate")                            

    item_df = item_df.withColumn("item_rate_percent", F.percent_rank().over(w).cast("double"))
```
<details>
  <summary>Показать расчет item_rate_percent</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/py_spark_seller_items/py_spark_raw_items_6.jpg" alt="Расчет item_rate_percent">
</details>
    
```python  
    item_df.write.mode("overwrite").parquet(TARGET_PATH)
    # Сохраняем преобразованный DataFrame в S3 в формате Parquet
    
    spark.stop()  # Останавливаем SparkSession

if __name__ == "__main__":
    main()
```

**Для запуска Spark задачи, оператору требуется указать пути до самой py-spark-job, а так же до конфигурационного файла задачи, где описаны параметры запуска.**

## Конфигурация SparkKubernetesOperator

```Python
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-job-sergej-vlasov-tnb4478-items # имя spark приложения
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: itayb/spark:3.1.1-hadoop-3.2.0-aws
  imagePullPolicy: Always
  mainApplicationFile: "local:///de-project/dags/sergej-vlasov-tnb4478/items-spark-job.py" # путь к Spark файлу
  sparkVersion: "3.1.1"
  timeToLiveSeconds: 40
  restartPolicy:
    type: Never
  volumes:
    - name: git-repo
      emptyDir:
        sizeLimit: 500Mi
    - name: ssh-key
      secret:
        secretName: ssh-key
        defaultMode: 256
  driver:
    tolerations:
      - key: k8s.karpov.courses/custom-11-12
        operator: Equal
        effect: NoSchedule
        value: 'true'
      - key: k8s.karpov.courses/custom-spark
        operator: Equal
        effect: NoSchedule
        value: 'true'
    volumeMounts:
      - name: "git-repo"
        mountPath: /de-project
      - name: ssh-key
        mountPath: /tmp/ssh
    initContainers:
      - name: git-clone
        image: alpine/git:2.40.1
        env:
          - name: GIT_SSH_COMMAND
            value: "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
        command: ['sh', '-c', 'git clone --depth=1 --single-branch git@git.lab.karpov.courses:de/de-project.git /de-project']
        volumeMounts:
          - name: git-repo
            mountPath: /de-project
          - name: ssh-key
            mountPath: /root/.ssh
    cores: 1
    coreLimit: "1200m"
    memory: "1024m"
    labels:
      version: 3.1.1
    serviceAccount: spark-driver
  executor:
    tolerations:
      - key: k8s.karpov.courses/custom-11-12
        operator: Equal
        effect: NoSchedule
        value: 'true'
      - key: k8s.karpov.courses/custom-spark
        operator: Equal
        effect: NoSchedule
        value: 'true'
    cores: 1
    coreLimit: "2500m"
    instances: 1
    memory: "2048m"
    labels:
      version: 3.1.1

```

<div style="display: flex; align-items: center; justify-content: space-between; width: 950px;">
  <table style="border-collapse: collapse;">
    <tr>
      <td style="padding: 0; text-align: center; border: none;">
        <a href="https://github.com/Vlasov-S-N-96/project/tree/main/startde-project/sergej-vlasov-tnb4478">
          <img src="https://raw.githubusercontent.com/Vlasov-S-N-96/project/main/icons/document.svg"
               alt="Нажми..."
               style="width: 50px; height: auto; transition: transform 0.2s ease-in-out; cursor: pointer;"
               onmouseover="this.style.transform='scale(1.1)';"
               onmouseout="this.style.transform='scale(1)';"
               title="Перейти к документу" />
        </a>
      </td>
      <td style="text-align: left; vertical-align: middle; padding-left: 10px; border: none;">
        startde-project
      </td>
    </tr>
  </table>

  <table style="border-collapse: collapse;">
    <tr>
      <td style="padding: 0; text-align: center; border: none;">
        <a href="https://github.com/Vlasov-S-N-96/project/blob/main/startde-project/de-project/sergej-vlasov-tnb4478/items-spark-job.py">
          <img src="https://raw.githubusercontent.com/Vlasov-S-N-96/project/main/icons/document.svg"
               alt="Нажми..."
               style="width: 50px; height: auto; transition: transform 0.2s ease-in-out; cursor: pointer;"
               onmouseover="this.style.transform='scale(1.1)';"
               onmouseout="this.style.transform='scale(1)';"
               title="Перейти к документу" />
        </a>
      </td>
      <td style="text-align: left; vertical-align: middle; padding-left: 10px; border: none;">
        items-spark-job.py
      </td>
    </tr>
  </table>

  <table style="border-collapse: collapse;">
    <tr>
      <td style="padding: 0; text-align: center; border: none;">
        <a href="https://github.com/Vlasov-S-N-96/project/blob/main/de-project/sergej-vlasov-tnb4478/items_spark_submit.yaml">
          <img src="https://raw.githubusercontent.com/Vlasov-S-N-96/project/main/icons/document.svg"
               alt="Нажми..."
               style="width: 50px; height: auto; transition: transform 0.2s ease-in-out; cursor: pointer;"
               onmouseover="this.style.transform='scale(1.1)';"
               onmouseout="this.style.transform='scale(1)';"
               title="Перейти к документу" />
        </a>
      </td>
      <td style="text-align: left; vertical-align: middle; padding-left: 10px; border: none;">
        items_spark_submit.yaml
      </td>
    </tr>
  </table>
</div>


## Разработка Airflow DAG 

```Python
import pendulum # для работы с датой и временем (start_date)
from airflow import DAG # импорт класса DAG
from airflow.operators.empty import EmptyOperator 
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator # запускает Spark приложения в кластере Kubernetes. 
# Принимает YAML файл с конфигурацией Spark приложения, отправляет его в Kubernetes, и ждет завершения приложения.
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor # отслеживать состояние Spark приложения, запущенного в Kubernetes. 
# Он периодически проверяет статус приложения и переходит к следующей задаче только после того, как приложение успешно завершится.
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime


K8S_SPARK_NAMESPACE = "de-project"  # Namespace Kubernetes, где будут запускаться Spark приложения
K8S_CONNECTION_ID = "kubernetes_karpov"  # ID соединения Airflow для подключения к Kubernetes
GREENPLUM_ID = "greenplume_karpov"  # ID соединения Airflow для подключения к Greenplum
SUBMIT_NAME = "job_submit"  # task_id для SparkKubernetesOperator
SENSOR_NAME = "job_sensor"  # task_id для SparkKubernetesSensor
```

# SQL-запрос для создания витрины данных в Greenplum 
```Sql
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
```
<details>
  <summary>Показать витрину данных sergej-vlasov-tnb4478".seller_items</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/Greenplum%20PXF/seller_items_1.jpg" alt="CREATE EXTERNAL TABLE sergej-vlasov-tnb4478.seller_items">
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/Greenplum%20PXF/seller_items_2.jpg" alt="CREATE EXTERNAL TABLE sergej-vlasov-tnb4478.seller_items">
</details>

```Sql
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
```
<details>
  <summary>Показать VIEW sergej-vlasov-tnb4478".unreliable_sellers_view</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/Greenplum%20PXF/create_unreliable_sellers_report_view.jpg" alt="CREATE VIEW sergej-vlasov-tnb4478.unreliable_sellers_view">
</details>

```Sql
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
```
<details>
  <summary>Показать VIEW sergej-vlasov-tnb4478".item_brands_view</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/Greenplum%20PXF/create_brands_report_view.jpg" alt="CREATE VIEW sergej-vlasov-tnb4478.item_brands_view">
</details>

# Функции для создания операторов
```Python
def _build_submit_operator(task_id: str, application_file: str, link_dag):
    """
    Создает и возвращает SparkKubernetesOperator для запуска Spark приложения.
    """
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True, 
        dag=link_dag
    )

def _build_sensor(task_id: str, application_name: str, link_dag):
    """
    Создает и возвращает SparkKubernetesSensor для отслеживания статуса Spark приложения.
    """
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,  # Прикрепляет логи Spark приложения к логам Airflow
        dag=link_dag
    )

# Аргументы по умолчанию для DAG
default_args = {
    "owner": "sergej-vlasov-tnb4478",  
}
```

# Создаем DAG
```Python 
with DAG(
    dag_id="startde-project-sergej-vlasov-tnb4478-dag",  # ID DAG 
    default_args=default_args,
    schedule_interval=None,  # DAG запускается вручную
    start_date=pendulum.datetime(2025, 3, 3, tz="UTC"),  # Дата начала
    tags=["job_submit"],  # Теги для удобной фильтрации
    catchup=False,  # Отключаем автоматический запуск пропущенных DAG Runs
) as dag:

    # --- Операторы ---
    # 1. SparkKubernetesOperator: Запускает Spark приложение на Kubernetes
    submit_task = _build_submit_operator(
        task_id=SUBMIT_NAME,
        application_file='items_spark_submit.yaml',  # YAML файл с конфигурацией Spark приложения
        link_dag=dag
    )

    # 2. SparkKubernetesSensor: Отслеживает статус Spark приложения
    sensor_task = _build_sensor(
        task_id=SENSOR_NAME,
        application_name=(
            "{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME}')['metadata']['name']}}}}"
        ),  # Получаем имя приложения из XCom (из submit_task)
        link_dag=dag
    )

    # 3. SQLExecuteQueryOperator: Создает витрину данных в Greenplum
    build_datamart = SQLExecuteQueryOperator(
        task_id="items_datamart",
        conn_id=GREENPLUM_ID,  # ID соединения Airflow для Greenplum
        sql=items_datamart_query,  # SQL запрос для создания витрины
        split_statements=True,  
        return_last=False,  
    )

   
    # Задачи выполняются последовательно:
    # 1. Запускаем Spark приложение
    # 2. Отслеживаем статус Spark приложения
    # 3. Создаем витрину данных в Greenplum
    # 4. После завершения build_datamart параллельно запускаются:item_brands_view,unreliable_sellers_view

    submit_task >> sensor_task >> build_datamart 
    build_datamart >> [item_brands_view,unreliable_sellers_view]
```
<details>
  <summary>Показать запуск DAGA</summary>
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/Airflow/Final_project.jpg" alt="DAG">
</details>

<style>
table {
  border: none !important;
  border-collapse: collapse !important;
}

td {
  border: none !important;
}
</style>

<table style="border-collapse: collapse;">
  <tr>
    <td style="padding: 0; text-align: center; border: none;">
      <a href="https://github.com/Vlasov-S-N-96/project/blob/main/de-project/sergej-vlasov-tnb4478/startde-project-sergej-vlasov-tnb4478-dag.py">
        <img src="https://raw.githubusercontent.com/Vlasov-S-N-96/project/main/icons/document.svg"
             alt="Нажми..."
             style="width: 50px; height: auto; transition: transform 0.2s ease-in-out; cursor: pointer;"
             onmouseover="this.style.transform='scale(1.1)';"
             onmouseout="this.style.transform='scale(1)';"
             title="Перейти к документу" />
      </a>
    </td>
    <td style="text-align: left; vertical-align: middle; padding-left: 10px; border: none;">
      startde-project-sergej-vlasov-tnb4478-dag.py
    </td>
  </tr>
</table>
