from datetime import date
from openbb import obb
import pandas as pd

def get_cotizaciones(tickers, start_date, end_date=None):
    """
    Descarga cotizaciones históricas para una lista de tickers usando OpenBB.
    
    Parámetros:
        tickers (list[str]): Lista de símbolos (ej: ["AAPL", "MSFT"])
        start_date (str): Fecha de inicio en formato YYYY-MM-DD
        end_date (str, opcional): Fecha de fin en formato YYYY-MM-DD (default: hoy)

    Retorna:
        pd.DataFrame con columnas: [Ticker, Fecha, Open, High, Low, Close, Volume]
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")

    all_data = []
    for symbol in tickers:
        try:
            df = obb.equity.price.historical(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            ).to_dataframe()
            df["Ticker"] = symbol  # agrega columna con el ticker
            all_data.append(df)
        except Exception as e:
            print(f"Error obteniendo {symbol}: {e}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()
