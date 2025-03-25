# Витрина Данных для Аналитиков KarpovZone

## Цель проекта

Предоставить аналитикам агрегированные метрики по карточкам товаром, популярным категориям и прочим параметрам на маркетплейсе KarpovZone. Цель также заключается в демонстрации совместной работы DWH (Greenplum), Data Lake (Spark) и Data Orchestrator (Airflow), приближенной к реальной работе Data Engineer.

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
*   **AWS S3 (или аналогичное):**  Для хранения исходных и промежуточных данных.
*   **Git/GitHub:** Для контроля версий кода.

## Исходные данные

Исходные данные размещены в S3 бакете `startde-raw` (VK.Cloud) в формате Parquet. Данные содержат информацию о товарах на маркетплейсе KarpovZone.

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
2.  **Разработка Spark Job (items-spark-job.py):**

    *   Чтение данных о товарах из S3 бакета `startde-raw/raw_items` в формате Parquet.
    *   Обогащение данных дополнительными параметрами и агрегатами:
        *   `returned_items_count` (количество товаров на которое оформлен возврат).
        *   `potential_revenue` (потенциальный доход от остатков товаров).
        *   `total_revenue` (доход от выполненных заказов с учетом возвратов).
        *   `avg_daily_sales` (среднее количество продаж с момента запуска).
        *   `days_to_sold` (количество дней которое потребуется для продажи всех доступных остатков товара).
        *   `item_rate_percent` (относительный ранг рейтинга товара).
    *   Сохранение результата в формате Parquet в S3 бакет `startde-project/{USER_LOGIN}/seller_items`.
3.  **Разработка Airflow DAG (startde-project-<student_id>-dag):**

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
            .config('spark.hadoop.fs.s3a.access.key', "r7LX3wSCP5ZK1yXupKEVVG")  # Access Key для S3
            .config('spark.hadoop.fs.s3a.secret.key', "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")  # Secret Key для S3
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
<a href="https://github.com/Vlasov-S-N-96/project/tree/main/startde-project/sergej-vlasov-tnb4478">
  <img src="https://github.com/Vlasov-S-N-96/project/blob/main/icons/document.svg"
       alt="Нажми..."
       style="width: 100px; /* Adjust the width as needed */
              height: auto; /* Maintain aspect ratio */
              transition: transform 0.2s ease-in-out; /* Smooth transition */
              cursor: pointer;"
       onmouseover="this.style.transform='scale(1.1)';"
       onmouseout="this.style.transform='scale(1)';"
       title="Перейти к проекту"
       />
</a>
