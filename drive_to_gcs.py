from google.cloud import storage
from google.cloud.storage import Blob

import io
import os 

import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ['https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_file('[JSON_CREDENTIALS]') # If you're using local machine
creds, _ = google.auth.default() #If you're using a cloud function directly
scoped_credentials = creds.with_scopes(SCOPES)
project_name = [PROJECT_NAME]


def get_blobs():
    client = storage.Client(project_name,creds) 
    bucket_name = client.get_bucket("[GCP_BUCKET_NAME]")
    objects_in_bucket = client.list_blobs(bucket_name, fields='items(name)')
    object_names = []
    for object in objects_in_bucket:
        object_names.append(object.name)
    return object_names

def search_file(request = None):
    service = build('drive', 'v3', credentials=scoped_credentials)

    files = []
    page_token = None
    file_to_download = None
    objects_list = get_blobs()
    while True:
        response = service.files().list(
                                            q=f"mimeType != 'application/vnd.google-apps.folder'",
                                            fields = 'nextPageToken, ''files(id,name)',
                                            pageToken=page_token
                                        ).execute()
        for file in response.get('files', []):
            print(F'Found file: {file.get("name")}, {file.get("id")}')
            file_id = file.get("id")
            file_name = file.get("name")
            if file_name not in objects_list:
                request = service.files().get_media(fileId=file_id)
                file_download_buffer = io.BytesIO()
                downloader = MediaIoBaseDownload(file_download_buffer, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    print(F'Download {int(status.progress() * 100)}.')
                
                upload_to_drive(file_download_buffer,file_name)

        files.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)


        if page_token is None:
            break

def upload_to_drive(file, file_name):
    client = storage.Client(project_name,creds)
    bucket_name = client.get_bucket("[GCP_BUCKET_NAME]")
    blob = Blob(file_name, bucket_name)
    blob.upload_from_file(file, rewind=True)

search_file()
