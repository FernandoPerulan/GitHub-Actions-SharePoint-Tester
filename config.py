import os

# --- Config desde secrets / env ---
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TENANT_ID = os.getenv("AZURE_TENANT_ID")
SHAREPOINT_HOSTNAME = os.getenv("SHAREPOINT_HOSTNAME")  # e.g. inspirareconsulting.sharepoint.com
SITE_SEARCH = os.getenv("SHAREPOINT_SITE_SEARCH")      # e.g. GrupoDONCAYETANO2