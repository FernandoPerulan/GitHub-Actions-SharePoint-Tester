
import requests
from urllib.parse import quote


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