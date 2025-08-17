# sharepoint_test.py
import os
import requests
from datetime import datetime
import pandas as pd
from urllib.parse import quote

# --- Config desde secrets / env ---
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TENANT_ID = os.getenv("AZURE_TENANT_ID")
SHAREPOINT_HOSTNAME = os.getenv("SHAREPOINT_HOSTNAME")  # e.g. inspirareconsulting.sharepoint.com
SITE_SEARCH = os.getenv("SHAREPOINT_SITE_SEARCH")      # e.g. GrupoDONCAYETANO2

# --- Helpers ---
def get_token(client_id, client_secret, tenant_id):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

def find_site_id(token, hostname, site_search):
    """
    Busca el site por nombre usando el endpoint /sites?search={q}
    Devuelve siteId (string) del primer resultado o None.
    """
    url = f"https://graph.microsoft.com/v1.0/sites?search={quote(site_search)}"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    items = r.json().get("value", [])
    # Buscar el que coincida con el hostname (filtrado simple)
    for it in items:
        web_url = it.get("webUrl", "")
        if hostname in web_url:
            return it.get("id")
    # si no encontramos por hostname devolvemos primer resultado si existe
    if items:
        return items[0].get("id")
    return None

def upload_file(token, site_id, remote_path, local_path):
    """
    Sube (PUT) un archivo pequeño directo a:
    /sites/{site_id}/drive/root:/{remote_path}:/content
    remote_path: ruta dentro de la librería, por ejemplo:
      'Tableros BI/Planillas de Input/Descargables SyC/test.xlsx'
    """
    # URL-encode la ruta manteniendo '/'
    remote_path_enc = quote(remote_path, safe="/")
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{remote_path_enc}:/content"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"}
    with open(local_path, "rb") as f:
        data = f.read()
    r = requests.put(url, headers=headers, data=data)
    r.raise_for_status()
    return r.json()

def download_file(token, site_id, remote_path, local_path):
    remote_path_enc = quote(remote_path, safe="/")
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{remote_path_enc}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, stream=True)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

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

    # Crear un xlsx de prueba con pandas
    df = pd.DataFrame({
        "Nombre": ["Fernando", "Ana"],
        "Valor": [1, 2],
        "Fecha": [datetime.today().strftime("%d-%m-%Y")] * 2
    })
    local_upload = "test_github.xlsx"
    df.to_excel(local_upload, index=False)
    print("Archivo local creado:", local_upload)

    # Ruta en SharePoint (ruta dentro de la librería)
    remote_path = "Tableros BI/Planillas de Input/Descargables SyC/test_github.xlsx"

    # Subir
    res = upload_file(token, site_id, remote_path, local_upload)
    print("Subida OK. Respuesta:", res.get("id"))

    # Descargar a archivo nuevo para validar
    local_download = "test_github_downloaded.xlsx"
    download_file(token, site_id, remote_path, local_download)
    print("Descargado OK a:", local_download)

if __name__ == "__main__":
    main()
