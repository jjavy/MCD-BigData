from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

def init_spark():
    """Inicializa Spark con límites de memoria para 16GB RAM."""
    return SparkSession.builder \
        .appName("StringDB_Network_Processor") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "12") \
        .getOrCreate()

def process_string_data(spark):
    print("Cargando enlaces de STRING-DB y limpiando prefijos...")
    # 1. Cargar Links y ELIMINAR el prefijo '9606.'
    links_df = spark.read \
        .option("header", "true") \
        .option("sep", " ") \
        .csv("../data/9606.protein.links.v12.0.txt.gz") \
        .withColumn("protein1", regexp_replace(col("protein1"), "^9606\\.", "")) \
        .withColumn("protein2", regexp_replace(col("protein2"), "^9606\\.", ""))
    
    print("Filtrando interacciones fuertes...")
    # 2. Filtrar interacciones de alta confianza (>= 500)
    strong_links = links_df \
        .withColumn("combined_score", col("combined_score").cast("int")) \
        .filter(col("combined_score") >= 500)
    
    print("Cargando diccionario de proteínas, renombrando columna y limpiando prefijos...")
    # 3. Cargar Info, renombrar la columna problemática y ELIMINAR el prefijo '9606.'
    info_df = spark.read \
        .option("header", "true") \
        .option("sep", "\t") \
        .csv("../data/9606.protein.info.v12.0.txt.gz") \
        .withColumnRenamed("#string_protein_id", "protein_id") \
        .withColumn("protein_id", regexp_replace(col("protein_id"), "^9606\\.", "")) \
        .select(col("protein_id"), col("preferred_name"))

    print("Realizando Join para mapear IDs a Nombres...")
    # 4. Join: Enriquecer los enlaces con nombres legibles
    enriched_df = strong_links.alias("l") \
        .join(info_df.alias("i1"), col("l.protein1") == col("i1.protein_id"), "inner") \
        .withColumnRenamed("preferred_name", "protein1_name") \
        .drop("protein_id")
        
    enriched_df = enriched_df.alias("e") \
        .join(info_df.alias("i2"), col("e.protein2") == col("i2.protein_id"), "inner") \
        .withColumnRenamed("preferred_name", "protein2_name") \
        .drop("protein_id")
        
    final_network = enriched_df.select(
        "protein1_name", 
        "protein2_name", 
        "combined_score"
    )

    print("Guardando resultados en formato Parquet...")
    # 5. Guardar el resultado en formato Parquet
    final_network.write \
        .mode("overwrite") \
        .parquet("../data/processed_network.parquet")
    
    print(f"Proceso completado. Total de interacciones guardadas: {final_network.count()}")

if __name__ == "__main__":
    spark_session = init_spark()
    process_string_data(spark_session)
    spark_session.stop()