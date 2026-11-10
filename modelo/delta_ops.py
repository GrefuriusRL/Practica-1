from delta.tables import DeltaTable

def convertir_parquet_a_delta(spark, ruta):
    """Ej 3: Convierte una carpeta Parquet a tabla Delta sin reescribir datos."""
    spark.sql(f"CONVERT TO DELTA parquet.`{ruta}`")

def obtener_version_actual(spark, ruta):
    """Obtiene de manera dinámica la versión actual del transaction log de Delta."""
    return DeltaTable.forPath(spark, ruta).history(1).select("version").collect()[0][0]

from delta.tables import DeltaTable

def ejecutar_merge_idempotente(spark, ruta_destino, df_nuevos):
    """Ej 4: Realiza un Merge / Upsert idempotente combinando actualización e inserción.
    Aplica dropDuplicates para evitar el error DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE.
    """
    # Eliminar duplicados en el origen para asegurar la cardinalidad 1-a-1 o N-a-1 en la clave del MERGE
    df_nuevos_unicos = df_nuevos.dropDuplicates(["Ticker", "Date"])
    
    dt = DeltaTable.forPath(spark, ruta_destino)
    dt.alias("d").merge(
        df_nuevos_unicos.alias("n"), "d.Ticker = n.Ticker AND d.Date = n.Date"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def eliminar_por_ticker(spark, ruta, ticker):
    """Ej 5c: Elimina filas de una tabla Delta basándose en una condición."""
    dt = DeltaTable.forPath(spark, ruta)
    dt.delete(f"Ticker = '{ticker}'")

def restaurar_version(spark, ruta, version):
    """Ej 6: Ejecuta un RESTORE a una versión histórica específica."""
    dt = DeltaTable.forPath(spark, ruta)
    dt.restoreToVersion(version)