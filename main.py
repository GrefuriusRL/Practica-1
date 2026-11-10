# ==============================================================================
# DECLARACIÓN DE USO DE IA:
# Este proyecto ha contado con el apoyo de herramientas de IA Generativa para la
# resolución de errores de ejecución (PySpark/Delta) y la estructuración del
# patrón de diseño MVC.
# ==============================================================================

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Importaciones del módulo Modelo
from modelo.ingesta import (
    TICKERS_IBEX, descargar_lote_ibex, descargar_sectores_ibex
)
from modelo.delta_ops import (
    convertir_parquet_a_delta, obtener_version_actual,
    ejecutar_merge_idempotente, eliminar_por_ticker, restaurar_version
)
from modelo.silver_cleaner import limpiar_sentimiento_silver, limpiar_ibex_silver
from modelo.gold_builder import construir_modelo_estrella

def main():
    # Inicializar Spark Session con soporte para Delta Lake (Clase 2)
    builder = SparkSession.builder \
        .appName("IbexDataLake_Medallion_MVC") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n==================================================================")
    print("CLASE 1: INGESTA Y CAPA BRONZE")
    print("==================================================================")

    # Ej 1-b
    print("\n[Ej 1-b] Descargando histórico IBEX a Bronze Parquet...")
    df_ibex_bronze = descargar_lote_ibex(TICKERS_IBEX, "2024-10-01", "2026-10-01", spark)
    ruta_b_ibex = "data/lake/bronze/ibex"
    df_ibex_bronze.write.mode("overwrite").parquet(ruta_b_ibex)

    # Ej 1-c
    print("\n[Ej 1-c] Demostración de Column Pruning:")
    df_pruned = spark.read.parquet(ruta_b_ibex).select("Ticker", "Date", "Close")
    df_pruned.show(5)

    # Ej 2-a & 2-b
    print("\n[Ej 2-a & 2-b] Lectura CSV Sentimiento y guardado en Bronze:")
    ruta_csv = "sentimiento_ibex.csv"
    df_sent_raw = spark.read.option("header", "true").option("delimiter", ";").csv(ruta_csv)
    print("Esquema crudo (Todo entra como String):")
    df_sent_raw.printSchema()
    
    ruta_b_sent = "data/lake/bronze/sentimiento"
    df_sent_raw.write.mode("overwrite").parquet(ruta_b_sent)
    print(f"Filas escritas en Bronze Sentimiento: {spark.read.parquet(ruta_b_sent).count()}")

    # Ej 2-c
    print("\n[Ej 2-c] Demostración Append en Parquet:")
    cnt_antes = spark.read.parquet(ruta_b_sent).count()
    df_sent_raw.write.mode("append").parquet(ruta_b_sent)
    cnt_despues = spark.read.parquet(ruta_b_sent).count()
    print(f"Filas antes del Append: {cnt_antes} | Filas después: {cnt_despues}")
    # Restauramos para mantener consistencia sin duplicados
    df_sent_raw.write.mode("overwrite").parquet(ruta_b_sent)

    print("\n==================================================================")
    print("CLASE 2: TABLAS DELTA, OPERACIONES Y TIME TRAVEL")
    print("==================================================================")

    # Ej 3
    print("\n[Ej 3] Convertir Parquet a Delta Lake:")
    convertir_parquet_a_delta(spark, ruta_b_ibex)
    convertir_parquet_a_delta(spark, ruta_b_sent)
    print("Historial de la tabla IBEX en Delta:")
    DeltaTable.forPath(spark, ruta_b_ibex).history().show()

    # Ej 4
    print("\n[Ej 4] MERGE / Upsert Idempotente:")
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    df_nuevos_precios = descargar_lote_ibex(TICKERS_IBEX, "2024-10-01", fecha_hoy, spark)

    ejecutar_merge_idempotente(spark, ruta_b_ibex, df_nuevos_precios)
    filas_m1 = spark.read.format("delta").load(ruta_b_ibex).count()

    ejecutar_merge_idempotente(spark, ruta_b_ibex, df_nuevos_precios)
    filas_m2 = spark.read.format("delta").load(ruta_b_ibex).count()
    
    print(f"Filas tras 1º Merge: {filas_m1} | Filas tras 2º Merge: {filas_m2} (Sin duplicados)")
    
    # CAPTURAR AQUÍ LA VERSIÓN EXACTA DEL EJERCICIO 4
    v_ej4 = obtener_version_actual(spark, ruta_b_ibex)
    print(f"-> Estado del Ejercicio 4 guardado en versión: {v_ej4}")

    # 1ª Ejecución MERGE
    ejecutar_merge_idempotente(spark, ruta_b_ibex, df_nuevos_precios)
    filas_m1 = spark.read.format("delta").load(ruta_b_ibex).count()

    # 2ª Ejecución MERGE (Idempotencia)
    ejecutar_merge_idempotente(spark, ruta_b_ibex, df_nuevos_precios)
    filas_m2 = spark.read.format("delta").load(ruta_b_ibex).count()
    
    print(f"Filas tras 1º Merge: {filas_m1} | Filas tras 2º Merge: {filas_m2} (Sin duplicados)")
    
    # Guardamos la versión dinámica tras finalizar el Ejercicio 4
    v_ej4 = obtener_version_actual(spark, ruta_b_ibex)
    print(f"-> Versión asignada al finalizar el Ej 4: Version {v_ej4}")

    # Ej 5
    print("\n[Ej 5-a & 5-b] Schema Enforcement y MergeSchema:")
    dt_ibex = DeltaTable.forPath(spark, ruta_b_ibex)
    df_ext = dt_ibex.toDF().withColumn("Close_mas5", F.col("Close") + 5)

    try:
        df_ext.write.format("delta").mode("append").save(ruta_b_ibex)
    except Exception:
        print("Fallo esperado: Schema enforcement bloqueó la columna no declarada.")

    df_ext.limit(5).write.format("delta").mode("append").option("mergeSchema", "true").save(ruta_b_ibex)
    print("Escritura con opción mergeSchema realizada correctamente.")

    # Ej 5-c
    print("\n[Ej 5-c] DELETE sobre Ticker BBVA.MC:")
    eliminar_por_ticker(spark, ruta_b_ibex, "BBVA.MC")
    print(f"Filas de BBVA.MC tras borrado: {spark.read.format('delta').load(ruta_b_ibex).filter(F.col('Ticker')=='BBVA.MC').count()}")

    # Ej 5-d
    print("\n[Ej 5-d] Time Travel (versionAsOf):")
    df_v_ej4 = spark.read.format("delta").option("versionAsOf", v_ej4).load(ruta_b_ibex)
    print(f"Filas de BBVA.MC consultadas en la versión {v_ej4}: {df_v_ej4.filter(F.col('Ticker')=='BBVA.MC').count()}")

    # Ej 6
    print("\n[Ej 6] RESTORE exacto a la versión del Ejercicio 4:")
    restaurar_version(spark, ruta_b_ibex, v_ej4)
    df_restaurado = spark.read.format("delta").load(ruta_b_ibex)
    print(f"Filas de BBVA.MC tras el RESTORE: {df_restaurado.filter(F.col('Ticker')=='BBVA.MC').count()}")

    print("\n==================================================================")
    print("CLASE 3: CAPAS SILVER, GOLD Y EXPORTACIÓN ESTRELLA")
    print("==================================================================")

    # Ej 8
    print("\n[Ej 8] Creación de Capa Silver Sentimiento:")
    df_b_sent = spark.read.format("delta").load(ruta_b_sent)
    df_s_sent = limpiar_sentimiento_silver(df_b_sent)
    ruta_s_sent = "data/lake/silver/sentimiento"
    df_s_sent.write.format("delta").mode("overwrite").save(ruta_s_sent)
    print(f"Filas generadas en Silver Sentimiento: {spark.read.format('delta').load(ruta_s_sent).count()}")

    # Ej 9
    print("\n[Ej 9] Creación de Capa Silver IBEX y Dimensión Empresa:")
    # Ingesta Bronze Dimensión Empresa
    sectores_datos = descargar_sectores_ibex()
    schema_emp = StructType([StructField("Ticker", StringType(), False), StructField("Sector", StringType(), True)])
    df_emp_b = spark.createDataFrame(sectores_datos, schema=schema_emp)
    df_emp_b.write.format("delta").mode("overwrite").save("data/lake/bronze/dim_empresa")

    # Limpieza Silver
    df_emp_s = df_emp_b.dropDuplicates(["Ticker"])
    df_emp_s.write.format("delta").mode("overwrite").save("data/lake/silver/dim_empresa")

    df_b_ibex = spark.read.format("delta").load(ruta_b_ibex)
    df_s_ibex = limpiar_ibex_silver(df_b_ibex)
    df_s_ibex.write.format("delta").mode("overwrite").save("data/lake/silver/ibex")

    print("Muestra de la Fila 5 de Silver IBEX:")
    df_s_ibex.limit(5).tail(1)

    # Ej 10
    print("\n[Ej 10] Generación de la Capa Gold (Modelo en Estrella):")
    df_gold_fact, df_gold_dim = construir_modelo_estrella(df_s_ibex, df_s_sent, df_emp_s)
    df_gold_fact.write.format("delta").mode("overwrite").save("data/lake/gold/fact_cotizaciones")
    df_gold_dim.write.format("delta").mode("overwrite").save("data/lake/gold/dim_empresa")
    print("Capa Gold guardada correctamente.")

    # Ej 13
    print("\n[Ej 13] Exportación a Parquet para Power BI:")
    fact_gold = spark.read.format("delta").load("data/lake/gold/fact_cotizaciones")
    dim_gold = spark.read.format("delta").load("data/lake/gold/dim_empresa")

    fact_gold.write.mode("overwrite").partitionBy("Ticker").parquet("data/export/gold/fact_cotizaciones")
    dim_gold.write.mode("overwrite").parquet("data/export/gold/dim_empresa")
    print("Exportación finalizada en data/export/gold")

if __name__ == "__main__":
    main()