from datetime import date
from openbb import obb
import pandas as pd

def get_cotizaciones(tickers, start_date, end_date=None, providers=None):
    """
    Descarga cotizaciones históricas para una lista de tickers usando OpenBB,
    probando varios providers hasta obtener datos.
    Formato tabular largo: Fecha | Ticker | Close | Dividend
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")

    if providers is None:
        providers = ["yfinance", "investing", "stooq", "polygon"]

    all_data = []

    for symbol in tickers:
        df = None
        for provider in providers:
            try:
                data = obb.equity.price.historical(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    provider=provider
                )
                df = data.to_dataframe() if data is not None else None
                if df is not None and not df.empty:
                    print(f"{symbol}: datos obtenidos con {provider}")
                    break  # encontramos datos, no probamos otros providers
            except Exception as e:
                print(f"{symbol}: no disponible en {provider} ({type(e).__name__})")
                continue

        if df is None or df.empty:
            print(f"{symbol}: sin datos en ningún provider, se salta.")
            continue

        # --- Normalizar columnas ---
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={"index": "date"})
        if "date" not in df.columns:
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df = df.rename(columns={col: "date"})
                    break

        close_col = next((c for c in ["Close","close","Adj Close","adjClose","adj_close","AdjClose"] if c in df.columns), None)
        if close_col is None:
            for c in df.columns:
                if c.lower() == "close":
                    close_col = c
                    break
        if close_col is None:
            print(f"{symbol}: no se encontró columna Close, se salta.")
            continue

        div_col = next((c for c in ["Dividends","dividends","Dividend","dividend"] if c in df.columns), None)

        out = pd.DataFrame({
            "Fecha": pd.to_datetime(df["date"]).dt.strftime("%d-%m-%Y"),
            "Ticker": symbol,
            "Close": df[close_col].astype(float),
            "Dividend": (df[div_col].astype(float) if div_col else 0.0)
        })

        all_data.append(out)

    if not all_data:
        return pd.DataFrame(columns=["Fecha","Ticker","Close","Dividend"])

    final = pd.concat(all_data, ignore_index=True)
    final["_fd"] = pd.to_datetime(final["Fecha"], format="%d-%m-%Y")
    final = final.sort_values(["Ticker","_fd"]).drop(columns="_fd").reset_index(drop=True)
    final = final[final["Close"].notna()].reset_index(drop=True)

    return final
