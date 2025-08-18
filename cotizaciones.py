# cotizaciones.py
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

def get_alpha_vantage(ticker, start_date, end_date=None):
    ts = TimeSeries(key=ALPHA_VANTAGE_KEY, output_format='pandas')
    try:
        data, _ = ts.get_daily_adjusted(symbol=ticker, outputsize='full')
        data = data.reset_index().rename(columns={
            "date": "Fecha",
            "5. adjusted close": "Close",
            "7. dividend amount": "Dividend"
        })
        data["Ticker"] = ticker
        data["Fecha"] = pd.to_datetime(data["Fecha"])
        data = data[(data["Fecha"] >= pd.to_datetime(start_date))]
        if end_date:
            data = data[data["Fecha"] <= pd.to_datetime(end_date)]
        data["Provider"] = "AlphaVantage"
        return data[["Fecha","Ticker","Close","Dividend","Provider"]]
    except Exception as e:
        print(f"{ticker} AlphaVantage error: {e}")
        return pd.DataFrame(columns=["Fecha","Ticker","Close","Dividend","Provider"])

def get_investing(ticker, country='argentina', start_date='01/01/2024', end_date=None):
    try:
        df = investpy.get_stock_historical_data(
            stock=ticker,
            country=country,
            from_date=start_date,
            to_date=end_date or pd.Timestamp.today().strftime('%d/%m/%Y')
        )
        df = df.reset_index().rename(columns={"Date":"Fecha","Close":"Close"})
        df["Ticker"] = ticker
        df["Dividend"] = 0.0
        df["Provider"] = "Investing"
        return df[["Fecha","Ticker","Close","Dividend","Provider"]]
    except Exception as e:
        print(f"{ticker} Investing error: {e}")
        return pd.DataFrame(columns=["Fecha","Ticker","Close","Dividend","Provider"])

def get_yfinance(ticker, start_date, end_date=None):
    try:
        df = obb.equity.price.historical(
            symbol=ticker,
            start_date=start_date,
            end_date=end_date,
            provider="yfinance"
        ).to_dataframe()
        
        # Normalizar columnas
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={"index":"Fecha"})
        close_col = next((c for c in ["Close","close","Adj Close","adjClose","adj_close","AdjClose"] if c in df.columns), None)
        div_col = next((c for c in ["Dividends","dividends","Dividend","dividend"] if c in df.columns), None)
        df_out = pd.DataFrame({
            "Fecha": pd.to_datetime(df["Fecha"]),
            "Ticker": ticker,
            "Close": df[close_col].astype(float),
            "Dividend": df[div_col].astype(float) if div_col else 0.0,
            "Provider": "YFinance"
        })
        return df_out
    except Exception as e:
        print(f"{ticker} YFinance error: {e}")
        return pd.DataFrame(columns=["Fecha","Ticker","Close","Dividend","Provider"])

def get_cotizaciones(tickers, start_date, end_date=None):
    all_data = []
    for ticker in tickers:
        df = None
        # Lista de providers en orden de prueba
        for provider in ["YFinance","AlphaVantage","Investing"]:
            if provider == "YFinance":
                df = get_yfinance(ticker, start_date, end_date)
            elif provider == "AlphaVantage":
                df = get_alpha_vantage(ticker, start_date, end_date)
            elif provider == "Investing":
                df = get_investing(ticker, start_date=start_date, end_date=end_date)
            
            if df is not None and not df.empty:
                print(f"{ticker}: datos obtenidos con {provider}")
                all_data.append(df)
                break
        else:
            print(f"{ticker}: sin datos en ningún provider")
    
    if all_data:
        final = pd.concat(all_data, ignore_index=True)
        final = final.sort_values(["Ticker","Fecha"]).reset_index(drop=True)
        return final
    else:
        return pd.DataFrame(columns=["Fecha","Ticker","Close","Dividend","Provider"])
