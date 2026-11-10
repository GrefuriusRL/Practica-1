import pandas as pd
import yfinance as yf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType, StringType

TICKERS_IBEX = [
    "ANA.MC", "AENA.MC", "ACS.MC", "ALM.MC", "MTS.MC", "BBVA.MC", "SAB.MC", "SAN.MC", 
    "BKT.MC", "CABK.MC", "CLNX.MC", "ENG.MC", "ELE.MC", "FER.MC", "FDR.MC", "GRF.MC", 
    "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "COL.MC", "LOG.MC", "MAP.MC", "MEL.MC", 
    "MRL.MC", "NTGY.MC", "PHM.MC", "RED.MC", "REP.MC", "ROVI.MC", "TRE.MC", "UNI.MC", "SCYR.MC"
]

def descargar_historico(ticker, fecha_i, fecha_f, spark):
    """Descarga el histórico de un ticker y crea un DataFrame de Spark asegurando el orden exacto de columnas."""
    pdf = yf.Ticker(ticker).history(start=fecha_i, end=fecha_f, interval="1d", auto_adjust=False)
    if pdf.empty:
        return None
      
    pdf.reset_index(inplace=True)
    pdf["Ticker"] = str(ticker)
    pdf.rename(columns={"Stock Splits": "Stock_Splits"}, inplace=True)

    pdf["Date"] = pd.to_datetime(pdf["Date"]).dt.date
    pdf["Volume"] = pdf["Volume"].fillna(0.0).astype("float64")
    pdf["Dividends"] = pdf["Dividends"].fillna(0.0).astype("float64")
    pdf["Stock_Splits"] = pdf["Stock_Splits"].fillna(0.0).astype("float64")

    # Ordenar columnas explícitamente para que coincidan con la posición en StructType
    cols_ordenadas = ["Date", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock_Splits", "Ticker"]
    pdf = pdf[cols_ordenadas]

    esquema = StructType([
        StructField("Date", DateType(), nullable=False),          
        StructField("Open", DoubleType(), nullable=True),        
        StructField("High", DoubleType(), nullable=True), 
        StructField("Low", DoubleType(), nullable=True),  
        StructField("Close", DoubleType(), nullable=False),
        StructField("Volume", DoubleType(), nullable=True),
        StructField("Dividends", DoubleType(), nullable=True),    
        StructField("Stock_Splits", DoubleType(), nullable=True),
        StructField("Ticker", StringType(), nullable=False)        
    ])

    df = spark.createDataFrame(pdf, schema=esquema)
    return df.withColumn("Volume", F.coalesce(F.round(F.col("Volume")).cast(LongType()), F.lit(0))) \
             .dropDuplicates(["Ticker", "Date"])

def descargar_lote_ibex(tickers, fecha_i, fecha_f, spark):
    """Descarga múltiples tickers y los agrupa en un único DataFrame."""
    df_acumulado = None
    for t in tickers:
        df_t = descargar_historico(t, fecha_i, fecha_f, spark)
        if df_t:
            df_acumulado = df_t if df_acumulado is None else df_acumulado.union(df_t)
    return df_acumulado

def descargar_sectores_ibex():
    """Descarga el sector empresarial de Yahoo Finance para cada ticker."""
    lista_sectores = []
    for ticker in TICKERS_IBEX:
        try:
            info = yf.Ticker(ticker).info
            sector = info.get("sector", "Desconocido")
        except Exception:
            sector = "Desconocido"
        lista_sectores.append((ticker, sector))
    return lista_sectores