# cotizaciones_fixed.py
from datetime import date
import pandas as pd

# YFinance via OpenBB
from openbb import obb

# Alpha Vantage
from alpha_vantage.timeseries import TimeSeries

# Investing.com
import investpy

# Tu API key de Alpha Vantage
ALPHA_VANTAGE_KEY = "UW78MNWY86HU6JEKEY"


def _find_column(df, patterns):
    """Busca en df.columns la primera columna cuyo nombre (minúsculas)
    contenga alguno de los patterns (también en minúsculas).
    Devuelve el nombre real de la columna o None."""
    cols = {c.lower(): c for c in df.columns}
    for p in patterns:
        for lower_col, real_col in cols.items():
            if p.lower() in lower_col:
                return real_col
    return None


def get_alpha_vantage(ticker, start_date, end_date=None):
    ts = TimeSeries(key=ALPHA_VANTAGE_KEY, output_format='pandas')
    try:
        data, _ = ts.get_daily_adjusted(symbol=ticker, outputsize='full')
        data = data.reset_index()
        # la primera columna tras reset_index suele ser la fecha
        data = data.rename(columns={data.columns[0]: "Fecha"})

        close_col = _find_column(data, ["adjusted close", "close"])
        div_col = _find_column(data, ["dividend", "dividend amount"])

        if close_col is None:
            raise ValueError("No se encontró columna de precio en AlphaVantage")

        out = pd.DataFrame({
            "Fecha": pd.to_datetime(data["Fecha"]),
            "Ticker": ticker,
            "Close": pd.to_numeric(data[close_col], errors='coerce'),
            "Dividend": pd.to_numeric(data[div_col], errors='coerce') if div_col else 0.0,
            "Provider": "AlphaVantage"
        })

        out = out[(out["Fecha"] >= pd.to_datetime(start_date))]
        if end_date is not None:
            out = out[out["Fecha"] <= pd.to_datetime(end_date)]

        out = out.dropna(subset=["Close"]).reset_index(drop=True)
        print(f"{ticker} AlphaVantage rows: {len(out)}")
        return out
    except Exception as e:
        print(f"{ticker} AlphaVantage error: {e}")
        return pd.DataFrame(columns=["Fecha", "Ticker", "Close", "Dividend", "Provider"]) 


def get_investing(ticker, country='argentina', start_date='01/01/2024', end_date=None):
    try:
        # investpy espera dd/mm/YYYY
        start = pd.to_datetime(start_date).strftime('%d/%m/%Y')
        to = (pd.to_datetime(end_date).strftime('%d/%m/%Y') if end_date is not None
              else pd.Timestamp.today().strftime('%d/%m/%Y'))

        df = investpy.get_stock_historical_data(
            stock=ticker,
            country=country,
            from_date=start,
            to_date=to
        )
        df = df.reset_index().rename(columns={df.columns[0]: "Fecha"})
        # buscar columna de cierre por si cambia el nombre
        close_col = _find_column(df, ["close", "adj close", "adj_close"])
        if close_col is None:
            raise ValueError("No se encontró columna Close en Investing")

        df["Fecha"] = pd.to_datetime(df["Fecha"])
        out = pd.DataFrame({
            "Fecha": df["Fecha"],
            "Ticker": ticker,
            "Close": pd.to_numeric(df[close_col], errors='coerce'),
            "Dividend": 0.0,
            "Provider": "Investing"
        })
        out = out.dropna(subset=["Close"]).reset_index(drop=True)
        print(f"{ticker} Investing rows: {len(out)}")
        return out
    except Exception as e:
        print(f"{ticker} Investing error: {e}")
        return pd.DataFrame(columns=["Fecha", "Ticker", "Close", "Dividend", "Provider"]) 


def get_yfinance(ticker, start_date, end_date=None):
    try:
        df = obb.equity.price.historical(
            symbol=ticker,
            start_date=start_date,
            end_date=end_date,
            provider="yfinance"
        ).to_dataframe()

        if df is None or df.empty:
            print(f"{ticker} YFinance returned empty")
            return pd.DataFrame(columns=["Fecha", "Ticker", "Close", "Dividend", "Provider"]) 

        # reset index si el índice es datetime
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            df = df.rename(columns={df.columns[0]: "Fecha"})
        else:
            # si ya tiene columna de fecha, normalizamos el nombre
            if 'Fecha' not in df.columns and 'date' in df.columns:
                df = df.rename(columns={'date': 'Fecha'})

        close_col = _find_column(df, ["adj close", "adjusted close", "close", "adjclose"])
        div_col = _find_column(df, ["dividend", "dividends"])

        if close_col is None:
            raise ValueError("No se encontró columna Close en YFinance/OpenBB")

        out = pd.DataFrame({
            "Fecha": pd.to_datetime(df["Fecha"]),
            "Ticker": ticker,
            "Close": pd.to_numeric(df[close_col], errors='coerce'),
            "Dividend": pd.to_numeric(df[div_col], errors='coerce') if div_col else 0.0,
            "Provider": "YFinance"
        })

        out = out[(out["Fecha"] >= pd.to_datetime(start_date))]
        if end_date is not None:
            out = out[out["Fecha"] <= pd.to_datetime(end_date)]

        out = out.dropna(subset=["Close"]).reset_index(drop=True)
        print(f"{ticker} YFinance rows: {len(out)}")
        return out
    except Exception as e:
        print(f"{ticker} YFinance error: {e}")
        return pd.DataFrame(columns=["Fecha", "Ticker", "Close", "Dividend", "Provider"]) 


def get_cotizaciones(tickers, start_date, end_date=None):
    all_data = []
    for ticker in tickers:
        df = None
        # Lista de providers en orden de prueba
        for provider in ["YFinance", "AlphaVantage", "Investing"]:
            if provider == "YFinance":
                df = get_yfinance(ticker, start_date, end_date)
            elif provider == "AlphaVantage":
                df = get_alpha_vantage(ticker, start_date, end_date)
            elif provider == "Investing":
                df = get_investing(ticker, start_date=start_date, end_date=end_date)

            if df is not None and not df.empty:
                print(f"{ticker}: datos obtenidos con {provider} ({len(df)} filas)")
                all_data.append(df)
                break
        else:
            print(f"{ticker}: sin datos en ningún provider")

    if all_data:
        final = pd.concat(all_data, ignore_index=True)
        final = final.sort_values(["Ticker", "Fecha"]).reset_index(drop=True)
        print(f"Total filas finales: {len(final)}")
        return final
    else:
        print("No se encontraron datos para ningún ticker.")
        return pd.DataFrame(columns=["Fecha", "Ticker", "Close", "Dividend", "Provider"]) 