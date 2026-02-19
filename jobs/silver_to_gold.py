from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum as spark_sum

# criando a sessão do Spark
def create_spark_session():
    return(
        SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    # leitura da silver 
    df = spark.read.parquet("data_lake/silver/transactions")

    # regra simples de fraude

    df_gold = df.withColumn(
        "fraud_flag",
        when(
            (col("amount") > 3000) &
            (col("is_international") == True) &
            (col("merchant_category") == "electronics"),
            1
        ).otherwise(0)
    )

    # metricas feitas por cliente
    metrics = (
        df_gold.groupBy("customer_id")
        .agg(
            count("*").alias("total_transactions"),
            spark_sum("amount").alias("total_amount"),
            spark_sum("fraud_flag").alias("fraud_count")
        )
    )

    df_gold.write.mode("overwrite").parquet("data_lake/gold/transactions")
    metrics.write.mode("overwrite").parquet("data_lake/gold/customer_metrics")

    print("Gold layer created successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
