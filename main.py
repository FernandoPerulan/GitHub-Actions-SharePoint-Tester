from datetime import datetime
import pandas as pd

# Credentials
import os
from auth import get_token
from sharepoint import find_site_id, upload_file, download_file
from cotizaciones import get_cotizaciones


# --- Config desde secrets / env ---
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TENANT_ID = os.getenv("AZURE_TENANT_ID")
SHAREPOINT_HOSTNAME = os.getenv("SHAREPOINT_HOSTNAME")  # e.g. inspirareconsulting.sharepoint.com
SITE_SEARCH = os.getenv("SHAREPOINT_SITE_SEARCH")      # e.g. GrupoDONCAYETANO2

# --- Main: crear xlsx de prueba, subir y descargar ---
def main():

    if not (CLIENT_ID and CLIENT_SECRET and TENANT_ID and SHAREPOINT_HOSTNAME and SITE_SEARCH):
        raise SystemExit("Faltan variables de entorno (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID, SHAREPOINT_HOSTNAME, SHAREPOINT_SITE_SEARCH)")

    token = get_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
    print("Token obtenido.")

    site_id = find_site_id(token, SHAREPOINT_HOSTNAME, SITE_SEARCH)
    if not site_id:
        raise SystemExit("No se encontró site_id para la búsqueda dada.")
    print("Site ID:", site_id)

    """# Crear un xlsx de prueba con pandas
    df = pd.DataFrame({
        "Nombre": ["Fernando", "Ana"],
        "Valor": [1, 2],
        "Fecha": [datetime.today().strftime("%d-%m-%Y")] * 2
    })
    """
    tickers = [
    "S13S4","T2X5","S29N4","BPY26","AL30","S30S4","S28F5","S13D4","S31E5","S31M5",
    "BPOB7","BPOC7","DIA","PAMP","IWM","MELI","YCA6O","BPOD7","S29G5","S18J5",
    "YPFD","EWZ","TEN","XLE","S30J5","YMCJO","S15G5","T17O5","T15D5","TZX26",
    "BBD","ABEV","GD35","GD30","GD41","CEPU","S30Y5","VALE","VIST","GOOGL",
    "TTJ26","TTM26","DE","PBR","JPM","NKE","TM","AL41","AL35","T2X4"]

    df = get_cotizaciones(tickers, start_date="2024-01-01")
    print("Final shape:", df.shape)

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
