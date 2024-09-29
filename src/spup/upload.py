from pathlib import Path
from typing import Optional
import requests
import os

# tenant id is the id of a microsoft organisation.
tenant_id = "abcdefghi-384c-4066-992a-coucouloucoucoupaloma"

# The client credentials are retrievable via the app created in https://portal.azure.com
# Azure AD app registration credentials
client_id = "blablablah"
client_secret = "clientp7sr3t"

# Name of the sharepoint
sharepoint_namespace = "poc-sharepoint"
# folder from inside the sharepoint
library_name = "anothertest"

# The following informations can be retrieved via the functions listed below
site_id = "mhy-super-site-id"
drive_id = "b!blipbloupiamarobout"

# Upload a file to the SharePoint document library using the Microsoft Graph API
file_path = "/tmp/requirements.txt"



def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    # Authenticate and get an access token
    auth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }
    response = requests.post(auth_url, data=data)
    access_token = response.json()["access_token"]
    return access_token

def upload_file(
    access_token: str,
    file_path: Path,
    site_id: str,
    drive_id: str,
    prefix: Optional[str] = None,
    file_name: Optional[str] = None,
):
    file_name = file_name or file_path.name
    prefix = "" if prefix is None else f"{prefix}/"
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/root:/{prefix}{file_name}:/content"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/octet-stream",
        "Content-Length": str(os.path.getsize(file_path)),
    }
    with open(file_path, "rb") as file:
        response = requests.put(upload_url, headers=headers, data=file)
        print(response.json())

if __name__ == "__main__":
    access_token = get_access_token(tenant_id, client_id, client_secret)
    upload_file(access_token, file_path, site_id, drive_id)
def get_site_ids(access_token: str, sharepoint_namespace: str) -> list[str]:
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    # r1 is for site_id
    site_response = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/root:/sites/{sharepoint_namespace}",
        headers=headers,
    )
    site_ids = site_response.json()["id"].split(",")
    return site_ids


def get_drive_id(access_token: str, site_id: str, library_name: str) -> list[str]:
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
    drive_response = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/",
        headers=headers,
    )
    for drive_metadata in drive_response.json()["value"]:
        if drive_metadata["name"] == library_name:
            return drive_metadata["id"]
