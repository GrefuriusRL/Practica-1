def construir_modelo_estrella(df_silver_ibex, df_silver_sent, df_silver_emp):
    """Ej 10: Cruza los precios cotizados con el sentimiento sectorial y la dimensión empresa."""
    df_gold_fact = df_silver_ibex \
        .join(df_silver_emp, on="Ticker", how="left") \
        .join(df_silver_sent, on=["Date", "Sector"], how="left")
    
    return df_gold_fact, df_silver_emp