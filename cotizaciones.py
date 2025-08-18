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

        # --- Normalizar columna de fecha ---
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={"index": "Fecha"})
        elif "date" in df.columns:
            df = df.rename(columns={"date": "Fecha"})
        else:
            # buscar cualquier columna datetime
            fecha_col = next((c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])), None)
            if fecha_col:
                df = df.rename(columns={fecha_col: "Fecha"})
            else:
                print(f"{symbol}: no se encontró columna de fecha, se salta.")
                continue

        # --- Detectar columna Close ---
        close_col = next((c for c in ["Close","close","Adj Close","adjClose","adj_close","AdjClose"] if c in df.columns), None)
        if close_col is None:
            for c in df.columns:
                if c.lower() == "close":
                    close_col = c
                    break
        if close_col is None:
            print(f"{symbol}: no se encontró columna Close, se salta.")
            continue

        # --- Detectar columna Dividend ---
        div_col = next((c for c in ["Dividends","dividends","Dividend","dividend"] if c in df.columns), None)

        # --- Crear DataFrame de salida ---
        out = pd.DataFrame({
            "Fecha": pd.to_datetime(df["Fecha"]).dt.strftime("%d-%m-%Y"),
            "Ticker": symbol,
            "Close": df[close_col].astype(float),
            "Dividend": df[div_col].astype(float) if div_col else 0.0
        })

        all_data.append(out)

    if not all_data:
        return pd.DataFrame(columns=["Fecha","Ticker","Close","Dividend"])

    final = pd.concat(all_data, ignore_index=True)
    final["_fd"] = pd.to_datetime(final["Fecha"], format="%d-%m-%Y")
    final = final.sort_values(["Ticker","_fd"]).drop(columns="_fd").reset_index(drop=True)
    final = final[final["Close"].notna()].reset_index(drop=True)

    return final
