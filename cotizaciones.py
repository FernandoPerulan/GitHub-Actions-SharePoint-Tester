from datetime import date
from openbb import obb
import pandas as pd

def get_cotizaciones(tickers, start_date, end_date=None):
    """
    Descarga cotizaciones históricas para una lista de tickers usando OpenBB,
    en formato largo (tabular), con columnas:
    Fecha | Ticker | Close | Dividend
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

            # Nos quedamos con fecha, cierre y dividendos si existen
            keep_cols = ["date", "close"]
            if "dividends" in df.columns:
                keep_cols.append("dividends")
            elif "dividend" in df.columns:
                keep_cols.append("dividend")

            df = df[keep_cols].copy()
            df["Ticker"] = symbol

            # Normalizamos nombres de columnas
            df = df.rename(columns={
                "date": "Fecha",
                "close": "Close",
                "dividends": "Dividend",
                "dividend": "Dividend"
            })

            # Aseguramos formato de fecha dd-mm-aaaa
            df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.strftime("%d-%m-%Y")

            all_data.append(df)

        except Exception as e:
            print(f"Error obteniendo {symbol}: {e}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame(columns=["Fecha", "Ticker", "Close", "Dividend"])
