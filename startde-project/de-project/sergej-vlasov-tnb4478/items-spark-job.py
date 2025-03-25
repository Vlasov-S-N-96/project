import io
import sys
import uuid
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql import types

USER = "sergej-vlasov-tnb4478"
DATA_PATH = "s3a://startde-raw/raw_items"  
TARGET_PATH = f"s3a://startde-project/{USER}/seller_items"

def _spark_session():
    return (SparkSession.builder
            .appName("SparkJob-items-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config('spark.hadoop.fs.s3a.access.key', "r7LX3wSCP5ZK1yXupKEVVG")
            .config('spark.hadoop.fs.s3a.secret.key', "3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw")
            .getOrCreate())

def main():
    spark = _spark_session()
    item_df = spark.read.parquet(DATA_PATH)

    w = Window.orderBy("item_rate")

    item_df = item_df.withColumn("returned_items_count", 
                                  F.round((F.col("ordered_items_count") * (1 - (F.col("avg_percent_to_sold") / 100))), 0).cast("int")) \
                     .withColumn("potential_revenue", 
                                 ((F.col("availability_items_count") + (F.col("ordered_items_count")) * F.col("item_price")).cast("bigint"))) \
                     .withColumn("total_revenue", 
                                 ((F.col("ordered_items_count") - F.col("returned_items_count") + F.col("goods_sold_count")) * F.col("item_price")).cast("bigint")) \
                     .withColumn("avg_daily_sales", 
                                 F.when(F.col("days_on_sell") > 0, (F.col("goods_sold_count") / F.col("days_on_sell"))).otherwise(0).cast("double")) \
                     .withColumn("days_to_sold", 
                                 F.when(F.col("avg_daily_sales") > 0, (F.col("availability_items_count") / F.col("avg_daily_sales"))).otherwise(None).cast("double")) \
                     .withColumn("item_rate_percent", F.percent_rank().over(w).cast("double"))

    item_df.write.mode("overwrite").parquet(TARGET_PATH)
    # 1. количество товаров на которое оформлен возврат
    # 2. потенциальных доход от остатков товаров и товаров в заказе
    # 3. доход от выполненных заказов с учетом возвратов
    # 4. среднее количество продаж с момента запуска
    # 5. количество дней которое потребуется для продажи всех доступных остатков товара
    # 6. Расчет percent_rank относительно рейтинга товара
    spark.stop()

if __name__ == "__main__":
    main()