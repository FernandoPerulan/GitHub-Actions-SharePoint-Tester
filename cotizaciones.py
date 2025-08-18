# cotizaciones.py
from datetime import date
from openbb import obb
import pandas as pd

def get_cotizaciones(tickers, start_date, end_date=None, providers=None):
    """
    Descarga cotizaciones históricas para una lista de tickers usando OpenBB,
    provider único: yfinance
    Columnas: Fecha, Ticker, Close, Dividend, Provider
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")

    if providers is None:
        providers = ["yfinance"]

    all_data = []
    suffixes = ["", ".BA", ".BCBA", ".SA"]

    for symbol in tickers:
        got = False
        for var in suffixes:
            sym = f"{symbol}{var}"
            for provider in providers:
                try:
                    df = obb.equity.price.historical(
                        symbol=sym,
                        start_date=start_date,
                        end_date=end_date,
                        provider=provider
                    ).to_dataframe()
                    if df is None or df.empty:
                        continue

                    # reset index si datetime
                    if isinstance(df.index, pd.DatetimeIndex):
                        df = df.reset_index()

                    # detectar columnas
                    fecha_col = next((c for c in df.columns if "date" in c.lower()), df.columns[0])
                    close_col = next((c for c in df.columns if "close" in c.lower()), None)
                    if close_col is None:
                        continue
                    div_col = next((c for c in df.columns if "dividend" in c.lower()), None)

                    out = pd.DataFrame({
                        "Fecha": pd.to_datetime(df[fecha_col]).dt.strftime("%d-%m-%Y"),
                        "Ticker": symbol,
                        "Close": df[close_col].astype(float),
                        "Dividend": df[div_col].astype(float) if div_col else 0.0,
                        "Provider": provider
                    })

                    all_data.append(out)
                    got = True
                    break
                except Exception:
                    continue
            if got:
                break

    if not all_data:
        return pd.DataFrame(columns=["Fecha","Ticker","Close","Dividend","Provider"])

    final = pd.concat(all_data, ignore_index=True)
    return final
