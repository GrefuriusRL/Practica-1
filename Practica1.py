import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType, StringType
from pyspark.sql import functions as F
import yfinance as yf

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# ==============================================================================
# Ej 1-a
# ==============================================================================
def descargar_h(ticker, fecha_i, fecha_f, spark_session):
    pdf = yf.Ticker(ticker).history(start=fecha_i, end=fecha_f, interval="1d", auto_adjust=False)
      
    pdf.reset_index(inplace=True)
    pdf["Ticker"] = ticker

    pdf.rename(columns={"Stock Splits": "Stock_Splits"}, inplace=True)

    pdf["Date"] = pdf["Date"].dt.tz_localize(None).dt.date
    pdf["Volume"] = pdf["Volume"].fillna(0.0).astype("float64")

    esquema_inicial = StructType([
        StructField("Date", DateType(), nullable=False),          
        StructField("Open", DoubleType(), nullable=False),        
        StructField("High", DoubleType(), nullable=False), 
        StructField("Low", DoubleType(), nullable=False),  
        StructField("Close", DoubleType(), nullable=False),
        StructField("Volume", DoubleType(), nullable=True),
        StructField("Dividends", DoubleType(), nullable=False),    
        StructField("Stock_Splits", DoubleType(), nullable=False), # Guión bajo en lugar de espacio
        StructField("Ticker", StringType(), nullable=False)        
    ])

    df = spark_session.createDataFrame(pdf, schema=esquema_inicial)
    df = df.withColumn("Volume", F.round(F.col("Volume")).cast(LongType()))
    
    return df

# ==============================================================================
# Ej 1-b
# ==============================================================================
def inicializar_bronze(spark):
    tickers_ibex = ["ANA.MC", "AENA.MC", "ACS.MC", "ALM.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", "BKT.MC", "CABK.MC", "CLNX.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "COL.MC", "LOG.MC", "MAP.MC", "MEL.MC", "MRL.MC", "NTGY.MC", "PHM.MC", "RED.MC", "REP.MC", "ROVI.MC", "TRE.MC", "UNI.MC", "SCYR.MC"]
    
    fecha_i = "2024-10-01"
    fecha_f = "2026-10-01"

    df_final = None

    print("\nComienza Ej 1-b")
    for ticker in tickers_ibex:
        df_ticker = descargar_h(ticker, fecha_i, fecha_f, spark)
    
        if df_final is None:
            df_final = df_ticker
        else:
            df_final = df_final.union(df_ticker)

    df_final.write.mode("overwrite").parquet("data/lake/bronze/ibex")
    print("\nExito")
    print("\nFinalizado Ej 1-b")

# ==============================================================================
# Ej 2-a y Ej 2-b
# ==============================================================================
def inicializar_sentimiento(spark):
    print("\nComienza Ej 2")

    ruta_csv = "sentimiento_ibex.csv"
    
    df_sentimiento_texto = spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .csv(ruta_csv)
    
    print("\nEsquema Ej 2-a\n")
    df_sentimiento_texto.printSchema()

    df_sentimiento_texto.write \
        .mode("overwrite") \
        .parquet("data/lake/bronze/sentimiento")
    
    df_bronze_sentimiento = spark.read.parquet("data/lake/bronze/sentimiento")

    print(f"Filas escritas Ej 2-b: {df_bronze_sentimiento.count()}")

# ==============================================================================
# Ej 1-c
# ==============================================================================
def ej1c(spark):
    print("Comienza EJ 1-c")

    df_completo = spark.read.parquet("data/lake/bronze/ibex")
    print("Esquema completo:")
    df_completo.printSchema()
    print("Primeras 5 filas completas:")
    df_completo.show(5)
    
    df_reducido = spark.read.parquet("data/lake/bronze/ibex").select("Ticker", "Date", "Close")
    print("Primeras 5 filas optimizadas:")
    df_reducido.show(5)

    print("Solo mostrando 3 columnas nos ahorramos que se muestre una tabla enorme por la cantidad de decimales o las columnas open y high que no interesan")

# ==============================================================================
# Ej 2-c
# ==============================================================================
def ej2c(spark):
    print("\nComienza Ej 2-c")
    ruta_parquet = "data/lake/bronze/sentimiento"
    
    filas_antes = spark.read.parquet(ruta_parquet).count()
    print(f"Filas ANTES del append: {filas_antes}")

    df_origen = spark.read.parquet(ruta_parquet)
    df_origen.write.mode("append").parquet(ruta_parquet)
    
    filas_despues = spark.read.parquet(ruta_parquet).count()
    print(f"Filas DESPUÉS del append: {filas_despues}")

    print("Aumenta el numero de filas a casi el doble")

# ==============================================================================
# Ej 3
# ==============================================================================
def ej3(spark):
    print("\nComienza Ej 3")
    
    ruta_ibex = "data/lake/bronze/ibex"
    ruta_sentimiento = "data/lake/bronze/sentimiento"

    spark.sql(f"CONVERT TO DELTA parquet.`{ruta_ibex}`")
    spark.sql(f"CONVERT TO DELTA parquet.`{ruta_sentimiento}`")
    print("Conversión a Delta realizada con éxito")

    dt_ibex = DeltaTable.forPath(spark, ruta_ibex)
    print("\nHistorial de Delta (IBEX):")
    dt_ibex.history().show()

    print("Se ha creado internamente la carpeta '_delta_log'.")
    print("Esta carpeta guarda los archivos JSON con los metadatos y el registro de transacciones")

# ==============================================================================
# Ej 4
# ==============================================================================
def ej4(spark):
    print("\nComienza Ej 4")
    ruta_ibex = "data/lake/bronze/ibex"
    dt_ibex = DeltaTable.forPath(spark, ruta_ibex)

    filas_inicio = dt_ibex.toDF().count()
    print(f"\nFilas antes del MERGE: {filas_inicio}")

    fecha_i = "2024-10-01"
    fecha_f = datetime.now().strftime("%Y-%m-%d")
    tickers_ibex = ["ANA.MC", "AENA.MC", "ACS.MC", "ALM.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", "BKT.MC", "CABK.MC", "CLNX.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "COL.MC", "LOG.MC", "MAP.MC", "MEL.MC", "MRL.MC", "NTGY.MC", "PHM.MC", "RED.MC", "REP.MC", "ROVI.MC", "TRE.MC", "UNI.MC", "SCYR.MC"]

    df_nuevo = None
    for ticker in tickers_ibex:
        df_ticker = descargar_h(ticker, fecha_i, fecha_f, spark)
        if df_nuevo is None:
            df_nuevo = df_ticker
        else:
            df_nuevo = df_nuevo.union(df_ticker)

    dt_ibex.alias("d") \
        .merge(df_nuevo.alias("n"), "d.Ticker = n.Ticker AND d.Date = n.Date") \
        .whenNotMatchedInsertAll() \
        .execute()

    filas_merge_1 = dt_ibex.toDF().count()
    print(f"\nFilas después de la 1ª ejecución: {filas_merge_1}")


    dt_ibex.alias("d") \
        .merge(df_nuevo.alias("n"), "d.Ticker = n.Ticker AND d.Date = n.Date") \
        .whenNotMatchedInsertAll() \
        .execute()

    filas_merge_2 = dt_ibex.toDF().count()
    print(f"Filas después de la 2ª ejecución: {filas_merge_2}")

    print("En la primera ejecución se insertan los días nuevos. En la segunda el número de filas no cambia debido a la idempotencia del merge")

# ==============================================================================
# Ej 5-a, Ej 5-b, Ej 5-c y Ej 5-d
# ==============================================================================
def ej5(spark):
    print("\nComienza Ej 5")
    ruta_ibex = "data/lake/bronze/ibex"

    
    print("\nEj 5-a")
    df_actual = spark.read.format("delta").load(ruta_ibex)
    df_con_columna = df_actual.withColumn("Close_mas5", F.col("Close") + 5)

    try:
        df_con_columna.write.format("delta").mode("append").save(ruta_ibex)
    except Exception as e:
        print("Error de esquema")

    
    print("\nEj 5-b")
    df_con_columna.limit(5).write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(ruta_ibex)
    print("Escritura completada con exito")

    
    print("\nEj 5-c")
    dt_ibex = DeltaTable.forPath(spark, ruta_ibex)
    
    filas_bbva_antes = dt_ibex.toDF().filter(F.col("Ticker") == "BBVA.MC").count()
    print(f"\nFilas de BBVA.MC antes: {filas_bbva_antes}")

    dt_ibex.delete("Ticker = 'BBVA.MC'")

    filas_bbva_despues = dt_ibex.toDF().filter(F.col("Ticker") == "BBVA.MC").count()
    print(f"\nFilas de BBVA.MC después: {filas_bbva_despues}")


    print("\nEj 5-d")
    print("\nHistorial de versiones:")
    dt_ibex.history().show()

    df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(ruta_ibex)
    
    print("\nEsquema en versionAsOf:")
    df_v1.printSchema()
    
    bbva_v1_count = df_v1.filter(F.col("Ticker") == "BBVA.MC").count()
    print(f"\nFilas de BBVA.MC en versionAsOf: {bbva_v1_count}")

# ==============================================================================
# Ej 6
# ==============================================================================
def ej6(spark):
    print("\nComienza Ej 6")
    ruta_ibex = "data/lake/bronze/ibex"
    dt_ibex = DeltaTable.forPath(spark, ruta_ibex)

    dt_ibex.restoreToVersion(1)
    print("\nTabla restaurada con exito a la version 1")

    print("\nHistorial tras el restore:")
    dt_ibex.history().show()

    df_restaurado = spark.read.format("delta").load(ruta_ibex)
    
    print("Esquema actual de la tabla:")
    df_restaurado.printSchema()

    bbva_restaurado = df_restaurado.filter(F.col("Ticker") == "BBVA.MC").count()
    print(f"\nFilas de BBVA.MC tras el RESTORE: {bbva_restaurado}")

    print("restore revierte la tabla al estado exacto que tenia en la versión indicada creando una nueva entrada en _delta_log.")

# ==============================================================================
# FUNCIONES MODELO (CLASE 3)
# ==============================================================================
def descargar_sectores_ibex():
    tickers_ibex = ["ANA.MC", "AENA.MC", "ACS.MC", "ALM.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", "BKT.MC", "CABK.MC", "CLNX.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "COL.MC", "LOG.MC", "MAP.MC", "MEL.MC", "MRL.MC", "NTGY.MC", "PHM.MC", "RED.MC", "REP.MC", "ROVI.MC", "TRE.MC", "UNI.MC", "SCYR.MC"]
    
    lista_sectores = []
    for ticker in tickers_ibex:
        info = yf.Ticker(ticker).info
        sector = info.get("sector", "Desconocido")
        lista_sectores.append((ticker, sector))
        
    return lista_sectores

def limpiar_sentimiento_silver(spark):
    df_bronze = spark.read.format("delta").load("data/lake/bronze/sentimiento")
    
    columnas_sector = [c for c in df_bronze.columns if c != "Fecha"]
    
    df_largo = df_bronze.unpivot(["Fecha"], columnas_sector, "Sector_ES", "Sentimiento_TXT")
    
    df_limpio = df_largo \
        .withColumn("Sector_Trim", F.trim(F.col("Sector_ES"))) \
        .withColumn("Sentimiento_Limpio", F.regexp_replace("Sentimiento_TXT", ",", ".")) \
        .withColumn("Sentimiento", F.col("Sentimiento_Limpio").try_cast(DoubleType())) \
        .withColumn("Date", F.to_date("Fecha", "yyyy-MM-dd")) \
        .dropna(subset=["Sentimiento", "Date"]) \
        .dropDuplicates(["Date", "Sector_Trim"])
    
    mapeo_sectores = F.create_map([
        F.lit("Finanzas"), F.lit("Financial Services"),
        F.lit("Energía"), F.lit("Energy"),
        F.lit("Utilities"), F.lit("Utilities"),
        F.lit("Telecom"), F.lit("Communication Services"),
        F.lit("Turismo"), F.lit("Consumer Cyclical"),
        F.lit("Inmobiliario"), F.lit("Real Estate"),
        F.lit("Industria"), F.lit("Industrials"),
        F.lit("Salud"), F.lit("Healthcare"),
        F.lit("Tecnología"), F.lit("Technology")
    ])
    
    df_silver = df_limpio.withColumn("Sector", F.coalesce(mapeo_sectores[F.col("Sector_Trim")], F.col("Sector_Trim"))) \
        .select("Date", "Sector", "Sentimiento")
        
    return df_silver

# ==============================================================================
# Ej 7
# ==============================================================================
def ej7():
    print("\nComienza Ej 7")
    print("Para integrar el experto con la información de Yahoo es necesario obtener el sector de cada empresa con yf.Ticker(ticker).info")

# ==============================================================================
# Ej 8
# ==============================================================================
def ej8(spark):
    print("\nComienza Ej 8")
    
    df_silver_sentimiento = limpiar_sentimiento_silver(spark)
    ruta_silver_sentimiento = "data/lake/silver/sentimiento"
    
    df_silver_sentimiento.write.format("delta").mode("overwrite").save(ruta_silver_sentimiento)
    
    filas = spark.read.format("delta").load(ruta_silver_sentimiento).count()
    print(f"Filas escritas en Silver Sentimiento: {filas}")

# ==============================================================================
# Ej 9
# ==============================================================================
def ej9(spark):
    print("\nComienza Ej 9")
    
    df_bronze_ibex = spark.read.format("delta").load("data/lake/bronze/ibex")
    df_silver_ibex = df_bronze_ibex.dropDuplicates(["Date", "Ticker"]).dropna(subset=["Close"])
    df_silver_ibex.write.format("delta").mode("overwrite").save("data/lake/silver/ibex")
    
    sectores_datos = descargar_sectores_ibex()
    esquema_empresa = StructType([
        StructField("Ticker", StringType(), False),
        StructField("Sector", StringType(), True)
    ])
    
    df_empresa_raw = spark.createDataFrame(sectores_datos, schema=esquema_empresa)
    
    df_empresa_raw.write.format("delta").mode("overwrite").save("data/lake/bronze/dim_empresa")
    df_empresa_raw.dropDuplicates(["Ticker"]).write.format("delta").mode("overwrite").save("data/lake/silver/dim_empresa")
    
    print("\nFila 5 de Silver IBEX:")
    df_silver_ibex.show(5)

# ==============================================================================
# Ej 10
# ==============================================================================
def ej10(spark):
    print("\nComienza Ej 10")
    
    df_ibex = spark.read.format("delta").load("data/lake/silver/ibex")
    df_sentimiento = spark.read.format("delta").load("data/lake/silver/sentimiento")
    df_empresa = spark.read.format("delta").load("data/lake/silver/dim_empresa")
    

    df_ibex_sector = df_ibex.join(df_empresa, on="Ticker", how="left")
    

    df_gold_fact = df_ibex_sector.join(df_sentimiento, on=["Date", "Sector"], how="left")
    

    df_gold_fact.write.format("delta").mode("overwrite").save("data/lake/gold/fact_cotizaciones")
    df_empresa.write.format("delta").mode("overwrite").save("data/lake/gold/dim_empresa")
    
    print("\nCapa Gold generada")

# ==============================================================================
# Ej 13
# ==============================================================================
def ej13(spark):
    print("\nComienza Ej 13")
    

    fact = spark.read.format("delta").load("data/lake/gold/fact_cotizaciones")
    dim_empresa = spark.read.format("delta").load("data/lake/gold/dim_empresa")

    fact.write.mode("overwrite").partitionBy("Ticker").parquet("data/export/gold/fact_cotizaciones")
    dim_empresa.write.mode("overwrite").parquet("data/export/gold/dim_empresa")
    
    print("Exportación a Parquet completada en data/export/gold")
    print("\nJustificación Ej 13:")
    print("Se exportan los datos en modelo en estrella particionando la tabla de hechos por 'Ticker'. Esto optimiza la lectura columnar en Power BI al aislar las búsquedas por empresa y reduce sustancialmente el I/O.")

# ==============================================================================
# Sesión Spark
# ==============================================================================
builder = SparkSession.builder \
    .appName("IbexDataLake_Bronze_Completo") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

if __name__ == "__main__":
    ## Clase 1
    inicializar_bronze(spark)
    ej1c(spark)
    inicializar_sentimiento(spark)
    ej2c(spark)

    ## Clase 2
    ej3(spark)
    ej4(spark)
    ej5(spark)
    ej6(spark)

    #Clase 3
    ej7()
    ej8(spark)
    ej9(spark)
    ej10(spark)
    ej13(spark)