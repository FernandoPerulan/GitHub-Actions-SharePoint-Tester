from datetime import date
from openbb import obb
import pandas as pd

def get_cotizaciones(tickers, start_date, end_date=None, providers=None):
    """
    Versión robusta que descarga cotizaciones históricas para 'tickers' probando varios providers.
    Devuelve formato largo: Fecha | Ticker | Close | Dividend | Provider
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")

    if providers is None:
        providers = ["yfinance", "investing"]

    all_data = []

    for symbol in tickers:
        df = None
        used_provider = None

        for provider in providers:
            try:
                data = obb.equity.price.historical(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    provider=provider
                )
                df = data.to_dataframe() if data is not None else None

                if df is None or df.empty:
                    print(f"{symbol}: provider {provider} devolvió vacío.")
                    continue

                # --- Aplanar columnas si hay MultiIndex (ej: algunos providers) ---
                df = df.copy()
                new_cols = []
                for c in df.columns:
                    if isinstance(c, tuple):
                        new_cols.append("_".join([str(x) for x in c if x is not None]))
                    else:
                        new_cols.append(str(c))
                df.columns = new_cols

                # --- Normalizar fecha ---
                if isinstance(df.index, pd.DatetimeIndex):
                    # mover índice datetime a columna
                    # si el índice tiene nombre, use ese, sino 'Fecha'
                    idx_name = df.index.name if df.index.name else "Fecha"
                    df = df.reset_index().rename(columns={idx_name: "Fecha"})
                elif any("date" in c.lower() or "time" in c.lower() for c in df.columns):
                    # renombrar la primera columna que parezca fecha a 'Fecha'
                    fecha_col = next(c for c in df.columns if "date" in c.lower() or "time" in c.lower())
                    df = df.rename(columns={fecha_col: "Fecha"})
                else:
                    # buscar columna con dtype datetime
                    fecha_col = next((c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])), None)
                    if fecha_col:
                        df = df.rename(columns={fecha_col: "Fecha"})
                    else:
                        print(f"{symbol}: {provider} - no se pudo identificar columna de fecha; se intenta siguiente provider.")
                        continue  # probar siguiente provider

                # --- Detectar columna Close ---
                lower_cols = [c.lower() for c in df.columns]
                close_candidates = [c for c in df.columns if "close" in c.lower()]
                # Priorizar "adj close" vs "close"
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
                    print(f"{symbol}: {provider} - no se encontró columna 'Close'; se intenta siguiente provider.")
                    continue

                # --- Detectar Dividend (opcional) ---
                div_candidates = [c for c in df.columns if "dividend" in c.lower() or "dividends" in c.lower()]
                div_col = div_candidates[0] if div_candidates else None

                # --- Convertir a numérico y chequear si hay datos válidos ---
                close_series = pd.to_numeric(df[preferred], errors="coerce")
                n_non_null = close_series.notna().sum()
                if n_non_null == 0:
                    print(f"{symbol}: {provider} - columna Close encontrada pero todos los valores son NaN. Probando siguiente provider.")
                    continue  # intentar siguiente provider

                # Todo ok: usamos este provider
                used_provider = provider

                # Crear salida parcial
                fecha_series = pd.to_datetime(df["Fecha"], errors="coerce")
                # Filtrar filas con fecha válida
                valid_mask = fecha_series.notna()
                fecha_series = fecha_series[valid_mask]
                close_series = close_series[valid_mask]
                if div_col:
                    dividend_series = pd.to_numeric(df.loc[valid_mask, div_col], errors="coerce").fillna(0.0)
                else:
                    dividend_series = pd.Series(0.0, index=fecha_series.index)

                out = pd.DataFrame({
                    "Fecha": fecha_series.dt.strftime("%d-%m-%Y"),
                    "Ticker": symbol,
                    "Close": close_series.astype(float).values,
                    "Dividend": dividend_series.astype(float).values,
                    "Provider": used_provider
                })

                print(f"{symbol}: {used_provider} -> filas totales obtenidas: {len(df)}, filas con Close válidos: {n_non_null}, filas con fecha válida: {len(out)}")

                all_data.append(out)
                break  # salir loop de providers porque ya conseguimos datos

            except Exception as e:
                print(f"{symbol}: error con provider {provider} -> {type(e).__name__}: {e}")
                continue

        if used_provider is None:
            print(f"{symbol}: sin datos en ningún provider. Se salta el ticker.")

    # Concatenar resultados
    if not all_data:
        return pd.DataFrame(columns=["Fecha", "Ticker", "Close", "Dividend", "Provider"])

    final = pd.concat(all_data, ignore_index=True)

    # Ordenar por Ticker y Fecha (parsear fecha para ordenar)
    final["_fd"] = pd.to_datetime(final["Fecha"], format="%d-%m-%Y", errors="coerce")
    final = final.sort_values(["Ticker", "_fd"]).drop(columns="_fd").reset_index(drop=True)

    # Mantener sólo filas con Close no nulo (ya vienen filtradas, pero aseguramos)
    final = final[final["Close"].notna()].reset_index(drop=True)

    return final
