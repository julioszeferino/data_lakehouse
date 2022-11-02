from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max

# Cria objeto da Spark Session
spark = (SparkSession.builder.appName("DeltaExercise")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Importa o modulo das tabelas delta
from delta.tables import *

# Leitura de dados
enem = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("delimiter", ";")
    .load("s3://julioszeferino-datalake-projeto01-tf/raw-data/enem")
)

# Escreve a tabela em staging em formato delta
print("Writing delta table...")
(
    enem
    .write
    .mode("overwrite") # escreve por cima se existir
    .format("delta") # formato delta
    .partitionBy("year")
    .save("s3://julioszeferino-datalake-projeto01-tf/staging-zone/enem") # endereco onde sera salvo
)