from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def limpiar_sentimiento_silver(df_bronze):
    """Ej 8: Procesa el CSV en crudo despivotando sectores y convirtiendo a tipos numéricos."""
    # 1. Eliminar filas que contengan nombres de cabeceras o texto invalido en la columna Fecha
    df_filtrado = df_bronze.filter(F.col("Fecha").rlike(r"^\d{4}-\d{2}-\d{2}$"))

    cols_sector = [c for c in df_filtrado.columns if c != "Fecha"]
    
    # 2. Despivotar
    df_unpivot = df_filtrado.unpivot(["Fecha"], cols_sector, "Sector_ES", "Sentimiento_TXT")
    
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

    # 3. Limpiar comas, usar try_cast para tolerar malformaciones y tipar a Double/Date
    return df_unpivot \
        .withColumn("Sector_Trim", F.trim(F.col("Sector_ES"))) \
        .withColumn("Sentimiento_Limpio", F.regexp_replace("Sentimiento_TXT", ",", ".")) \
        .withColumn("Sentimiento", F.col("Sentimiento_Limpio").try_cast(DoubleType())) \
        .withColumn("Date", F.to_date("Fecha", "yyyy-MM-dd")) \
        .withColumn("Sector", F.coalesce(mapeo_sectores[F.col("Sector_Trim")], F.col("Sector_Trim"))) \
        .dropna(subset=["Sentimiento", "Date"]) \
        .dropDuplicates(["Date", "Sector"]) \
        .select("Date", "Sector", "Sentimiento")

def limpiar_ibex_silver(df_bronze):
    """Ej 9: Limpia los precios duplicados o faltantes para la capa Silver."""
    return df_bronze.dropna(subset=["Close"]).dropDuplicates(["Date", "Ticker"])