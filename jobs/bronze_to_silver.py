from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# criando a sessão do Spark
def create_spark_session():
    return(
        SparkSession.builder \
        .appName("BronzeToSilver") \
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    # leitura da bronze 
    df = spark.read.json("data_lake/bronze/transactions.json")

    # transformação dos dados
    df_silver = (
        df.withColumn("amount", col("amount").cast("double")) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("is_international", col("is_international").cast("boolean"))
    )

    df_silver.write.mode("overwrite").parquet("data_lake/silver/transactions")

    print("Silver layer created successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
