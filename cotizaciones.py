# cotizaciones_debug.py
from datetime import date
from openbb import obb
import pandas as pd

def get_cotizaciones_debug(tickers, start_date, end_date=None, providers=None, save_debug_csv=None):
    """
    Versión con logging y prueba de variantes de símbolo.
    Devuelve: DataFrame con columnas Fecha, Ticker, Close, Dividend, Provider
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")

    if providers is None:
        providers = ["yfinance"]  # por defecto; podés añadir más si tenés keys

    # variantes por ticker para mercados locales (prueba en este orden)
    suffixes = ["", ".BA", ".BCBA", ".SA"]

    all_data = []

    for symbol in tickers:
        print(f"\n=== TICKER: {symbol} ===")
        got = False

        # probar variantes del símbolo
        for var_suffix in suffixes:
            sym = f"{symbol}{var_suffix}"
            for provider in providers:
                try:
                    print(f"Intentando provider={provider}, symbol='{sym}' (start={start_date} end={end_date})")
                    data = obb.equity.price.historical(
                        symbol=sym,
                        start_date=start_date,
                        end_date=end_date,
                        provider=provider
                    )
                    df = data.to_dataframe() if data is not None else None

                    if df is None:
                        print(f" -> provider {provider} devolvió None para '{sym}'")
                        continue
                    if df.empty:
                        print(f" -> provider {provider} devolvió DataFrame vacío para '{sym}'")
                        continue

                    # aplanar columnas si MultiIndex
                    df = df.copy()
                    new_cols = []
                    for c in df.columns:
                        if isinstance(c, tuple):
                            new_cols.append("_".join([str(x) for x in c if x is not None]))
                        else:
                            new_cols.append(str(c))
                    df.columns = new_cols

                    print(f" -> filas devueltas: {len(df)}, columnas: {list(df.columns)[:12]}")

                    # mover índice datetime a columna si existe
                    if isinstance(df.index, pd.DatetimeIndex):
                        df = df.reset_index()

                    # detectar columna de fecha
                    fecha_col = None
                    for c in df.columns:
                        if "date" in c.lower() or "time" in c.lower():
                            fecha_col = c
                            break
                    if fecha_col is None:
                        fecha_col = next((c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])), None)

                    if fecha_col is None:
                        print(f" -> NO se detectó columna de fecha en provider {provider} / '{sym}'. columnas: {df.columns}")
                        continue
                    print(f" -> fecha detectada en columna '{fecha_col}' (dtype {df[fecha_col].dtype})")

                    # detectar close
                    close_candidates = [c for c in df.columns if "close" in c.lower()]
                    preferred = None
                    for name in ["adj close", "adjclose", "adj_close"]:
                        for c in close_candidates:
                            if name in c.lower():
                                preferred = c
                                break
                        if preferred:
                            break
                    if not preferred and close_candidates:
                        preferred = close_candidates[0]
                    if not preferred:
                        print(f" -> NO se detectó columna 'Close' en provider {provider} / '{sym}'.")
                        continue
                    print(f" -> Close detectado en '{preferred}'")

                    # preparar series
                    fecha_s = pd.to_datetime(df[fecha_col], errors="coerce")
                    close_s = pd.to_numeric(df[preferred], errors="coerce")
                    valid_mask = fecha_s.notna() & close_s.notna()
                    print(f" -> filas con fecha válida: {fecha_s.notna().sum()}, filas con Close válido: {close_s.notna().sum()}, filas válidas después de mask: {valid_mask.sum()}")

                    if valid_mask.sum() == 0:
                        print(" -> No hay filas válidas (fecha y close). Probar siguiente.")
                        continue

                    # dividend si existe
                    div_candidates = [c for c in df.columns if "dividend" in c.lower()]
                    div_col = div_candidates[0] if div_candidates else None

                    fecha_final = fecha_s[valid_mask].dt.strftime("%d-%m-%Y")
                    close_final = close_s[valid_mask].astype(float)
                    if div_col:
                        dividend_final = pd.to_numeric(df.loc[valid_mask, div_col], errors="coerce").fillna(0.0).astype(float)
                    else:
                        dividend_final = pd.Series(0.0, index=fecha_final.index)

                    out = pd.DataFrame({
                        "Fecha": fecha_final.values,
                        "Ticker": symbol,
                        "Close": close_final.values,
                        "Dividend": dividend_final.values,
                        "Provider": provider
                    })

                    print(f" -> OK: obtenidas {len(out)} filas para '{symbol}' usando '{sym}'/@{provider}. Ejemplo:\n{out.head(3)}")
                    all_data.append(out)
                    got = True
                    break  # salir provider loop

                except Exception as e:
                    print(f" -> Exception provider={provider}, symbol='{sym}': {type(e).__name__}: {e}")
                    continue

            if got:
                break  # salir variant loop

        if not got:
            print(f"!!! No se obtuvieron datos para {symbol} con ninguna variante/provider.")

    if not all_data:
        print("Resultado final: NO se obtuvieron datos para ningún ticker.")
        return pd.DataFrame(columns=["Fecha", "Ticker", "Close", "Dividend", "Provider"])

    final = pd.concat(all_data, ignore_index=True)
    final["_fd"] = pd.to_datetime(final["Fecha"], format="%d-%m-%Y", errors="coerce")
    final = final.sort_values(["Ticker", "_fd"]).drop(columns="_fd").reset_index(drop=True)
    final = final[final["Close"].notna()].reset_index(drop=True)

    print(f"\nData final: filas={len(final)}, columnas={list(final.columns)}")
    print(final.head(10))

    if save_debug_csv:
        final.to_csv(save_debug_csv, index=False)
        print(f"Debug CSV guardado: {save_debug_csv}")

    return final
