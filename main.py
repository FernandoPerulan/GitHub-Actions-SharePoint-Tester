from datetime import datetime
import pandas as pd

# Credentials
from auth import get_token
from sharepoint import find_site_id, upload_file, download_file
from config import AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID, SHAREPOINT_HOSTNAME, SHAREPOINT_SITE_SEARCH
from cotizaciones import get_cotizaciones

# --- Main: crear xlsx de prueba, subir y descargar ---
def main():
   
    if not (AZURE_CLIENT_ID and AZURE_CLIENT_SECRET and AZURE_TENANT_ID and SHAREPOINT_HOSTNAME and SHAREPOINT_SITE_SEARCH):
        raise SystemExit("Faltan variables de entorno (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID, SHAREPOINT_HOSTNAME, SHAREPOINT_SITE_SEARCH)")

    token = get_token(AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
    print("Token obtenido.")

    site_id = find_site_id(token, SHAREPOINT_HOSTNAME, SHAREPOINT_SITE_SEARCH)
    if not site_id:
        raise SystemExit("No se encontró site_id para la búsqueda dada.")
    print("Site ID:", site_id)

    # Crear un xlsx de prueba con pandas
    df = pd.DataFrame({
        "Nombre": ["Fernando", "Ana"],
        "Valor": [1, 2],
        "Fecha": [datetime.today().strftime("%d-%m-%Y")] * 2
    })

    tickers = ["AAPL", "MSFT", "TSLA"]
    df = get_cotizaciones(tickers, start_date="2024-01-01")

    local_upload = "cotizaciones.xlsx"
    df.to_excel(local_upload, index=False)
    print("Archivo local creado:", local_upload)

    # Ruta en SharePoint (ruta dentro de la librería)
    remote_path = "Tableros BI/Planillas de Input/Descargables SyC/cotizaciones.xlsx"

    # Subir
    res = upload_file(token, site_id, remote_path, local_upload)
    print("Subida OK. Respuesta:", res.get("id"))

    ## Descargar a archivo nuevo para validar
    #local_download = "test_github_downloaded.xlsx"
    #download_file(token, site_id, remote_path, local_download)
    #print("Descargado OK a:", local_download)

if __name__ == "__main__":
    main()
