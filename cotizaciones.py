from datetime import date
from openbb import obb
import pandas as pd

def get_cotizaciones(tickers, start_date, end_date=None, providers=None):
    """
    Descarga cotizaciones históricas en formato largo:
    Fecha | Ticker | Close | Dividend | Provider

    Por defecto usa un solo provider: "yfinance". La estructura permite
    pasar más providers en la lista `providers` si querés ampliarla después.
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")

    if providers is None:
        providers = ["yfinance"]  # por defecto, fácil de extender

    all_data = []

    for symbol in tickers:
        used_provider = None
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

                if df is None or df.empty:
                    # provider no trajo datos, probar siguiente
                    continue

                # normalizar columnas (aplanar MultiIndex si corresponde)
                df = df.copy()
                new_cols = []
                for c in df.columns:
                    if isinstance(c, tuple):
                        new_cols.append("_".join([str(x) for x in c if x is not None]))
                    else:
                        new_cols.append(str(c))
                df.columns = new_cols

                # Normalizar fecha: buscar una columna datetime (o mover índice)
                if isinstance(df.index, pd.DatetimeIndex):
                    df = df.reset_index()
                # identificar columna de fecha (priorizar nombres que contengan 'date' o 'time')
                fecha_col = None
                for c in df.columns:
                    if "date" in c.lower() or "time" in c.lower():
                        fecha_col = c
                        break
                if fecha_col is None:
                    fecha_col = next((c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])), None)
                if fecha_col is None:
                    # no hay fecha identificable en este provider: probar siguiente
                    continue

                # detectar columna Close (priorizar 'adj close' si existe)
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
                    # no hay columna close: probar siguiente provider
                    continue

                # detectar dividend (opcional)
                div_candidates = [c for c in df.columns if "dividend" in c.lower()]
                div_col = div_candidates[0] if div_candidates else None

                # convertir series y filtrar filas con fecha válida y close válido
                fecha_s = pd.to_datetime(df[fecha_col], errors="coerce")
                close_s = pd.to_numeric(df[preferred], errors="coerce")
                valid_mask = fecha_s.notna() & close_s.notna()
                if valid_mask.sum() == 0:
                    # no hay filas válidas con este provider
                    continue

                # preparar series finales
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

                all_data.append(out)
                used_provider = provider
                break  # ya obtuvimos datos para este ticker

            except Exception as e:
                # si falla el provider, seguir con el siguiente sin romper todo
                print(f"{symbol}: error con provider {provider} -> {type(e).__name__}: {e}")
                continue

        if used_provider is None:
            print(f"{symbol}: sin datos en los providers solicitados.")

    # concatenar resultados y ordenar
    if not all_data:
        return pd.DataFrame(columns=["Fecha", "Ticker", "Close", "Dividend", "Provider"])

    final = pd.concat(all_data, ignore_index=True)
    # asegurar orden por Ticker y Fecha (parsear Fecha para ordenar)
    final["_fd"] = pd.to_datetime(final["Fecha"], format="%d-%m-%Y", errors="coerce")
    final = final.sort_values(["Ticker", "_fd"]).drop(columns="_fd").reset_index(drop=True)

    # columnas en el orden pedido
    final = final[["Fecha", "Ticker", "Close", "Dividend", "Provider"]]

    return final
