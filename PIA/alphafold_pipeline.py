from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, explode, split

def init_spark():
    return SparkSession.builder \
        .appName("Master_Mapping_Robust") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()

def process_mapping(spark):
    print("1. Cargando archivo de mapeo de UniProt...")
    uniprot_df = spark.read \
        .option("sep", "\t") \
        .csv("../data/idmapping_selected.tab.gz")
    
    print("2. Filtrando proteoma humano y extrayendo Ensembl_PRO...")
    # CORRECCIÓN: _c20 es la columna real de Ensembl_PRO
    human_uniprot = uniprot_df \
        .filter(col("_c12") == "9606") \
        .select(
            col("_c0").alias("uniprot_id"),
            col("_c20").alias("ensembl_pro") 
        ) \
        .filter(col("ensembl_pro").isNotNull())
    
    human_uniprot = human_uniprot \
        .withColumn("ensembl_pro", explode(split(col("ensembl_pro"), "; "))) \
        .withColumn("ensembl_pro", regexp_replace(col("ensembl_pro"), " ", ""))

    # CHECKPOINT 1
    print(f" -> CHECKPOINT: Proteínas humanas con ID de Ensembl encontradas: {human_uniprot.count()}")

    print("3. Cargando diccionario de STRING-DB...")
    string_info = spark.read \
        .option("header", "true") \
        .option("sep", "\t") \
        .csv("../data/9606.protein.info.v12.0.txt.gz") \
        .withColumnRenamed("#string_protein_id", "protein_id") \
        .withColumn("protein_id", regexp_replace(col("protein_id"), "^9606\\.", "")) \
        .select(
            col("protein_id").alias("string_ensp"), 
            col("preferred_name")
        )
        
    print("4. Creando Piedra Rosetta (Join Info <-> UniProt)...")
    translation_table = string_info.join(
        human_uniprot,
        string_info.string_ensp == human_uniprot.ensembl_pro,
        "inner"
    ).select("preferred_name", "uniprot_id").dropDuplicates(["preferred_name"])

    # CHECKPOINT 2
    print(f" -> CHECKPOINT: Nombres traducidos exitosamente: {translation_table.count()}")

    print("5. Cruzando con la red de interacciones (Parquet base)...")
    network_df = spark.read.parquet("../data/processed_network.parquet")
    
    master_df = network_df.join(
        translation_table,
        network_df.protein1_name == translation_table.preferred_name,
        "inner"
    ).select(
        "protein1_name", 
        "protein2_name", 
        "combined_score", 
        col("uniprot_id").alias("p1_uniprot")
    )

    # CHECKPOINT 3 (El más importante)
    final_count = master_df.count()
    print(f" -> CHECKPOINT FINAL: Interacciones listas para Hugging Face: {final_count}")

    if final_count > 0:
        print("Guardando master_mapping.parquet...")
        master_df.write.mode("overwrite").parquet("../data/master_mapping.parquet")
        print("¡Éxito total!")
    else:
        print("ERROR: El cruce resultó en 0 filas. No se guardará el Parquet para evitar sobrescribir con vacío.")

if __name__ == "__main__":
    spark_session = init_spark()
    process_mapping(spark_session)
    spark_session.stop()