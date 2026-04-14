from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, min as spark_min

def init_spark():
    return SparkSession.builder \
        .appName("Global_Network_Analytics") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

def calculate_centrality(spark):
    print("Cargando red completa para análisis de importancia...")
    network_df = spark.read.parquet("../data/processed_network.parquet")

    # Calcular el grado (número de vecinos) para cada proteína
    # Se unen protein1 y protein2 para contar todas las apariciones de cada gen
    p1 = network_df.select(col("protein1_name").alias("protein"))
    p2 = network_df.select(col("protein2_name").alias("protein"))

    all_nodes = p1.union(p2)
    centrality_df = all_nodes.groupBy("protein").agg(count("*").alias("degree"))

    # Normalización del Score (0 a 100) para que sea fácil de entender en la App
    max_deg = centrality_df.select(spark_max("degree")).collect()[0][0]
    min_deg = centrality_df.select(spark_min("degree")).collect()[0][0]

    print(f"Proteína más conectada tiene {max_deg} conexiones.")

    final_metrics = centrality_df.withColumn(
        "influence_score", 
        ((col("degree") - min_deg) / (max_deg - min_deg)) * 100
    )

    # Guardar el ranking global
    print("Guardando ranking de influencia...")
    final_metrics.sort(col("influence_score").desc()) \
        .write.mode("overwrite") \
        .parquet("../data/global_metrics.parquet")
    
    print("¡Análisis completado!")

if __name__ == "__main__":
    spark = init_spark()
    calculate_centrality(spark)
    spark.stop()
