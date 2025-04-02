# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import proto
from gcs_jupyter_plugin import urls
from gcs_jupyter_plugin.commons.constants import CONTENT_TYPE, STORAGE_SERVICE_NAME
from google.oauth2 import credentials
from google.cloud import storage


class Client:
    def __init__(self, credentials, log, client_session):
        self.log = log
        if not (
            ("access_token" in credentials)
            and ("project_id" in credentials)
            and ("region_id" in credentials)
        ):
            self.log.exception("Missing required credentials")
            raise ValueError("Missing required credentials")
        self._access_token = credentials["access_token"]
        self.project_id = credentials["project_id"]
        self.region_id = credentials["region_id"]
        self.client_session = client_session

    async def list_buckets(self, prefix=None):
        try:
            bucket_list = []
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)
            buckets = client.list_buckets()
            buckets = client.list_buckets(prefix=prefix)
            for bucket in buckets:
                bucket_list.append(
                    {
                        "items": {
                            "name": bucket.name,
                            "updated": (
                                bucket.updated.isoformat() if bucket.updated else ""
                            ),
                        }
                    }
                )
            return bucket_list
        except Exception as e:
            self.log.exception("Error fetching datasets list")
            return {"error": str(e)}

    async def create_folder(self, bucket, path, folder_name):
        try:
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)

            # Format the folder path
            new_folder_path = (
                folder_name + "/" if path == "" else path + "/" + folder_name + "/"
            )

            # Get the bucket
            bucket_obj = client.bucket(bucket)
            # Create an empty blob with a trailing slash to indicate a folder
            blob = bucket_obj.blob(new_folder_path)
            # Upload empty content to create the folder
            blob.upload_from_string("")

            # Return the folder information
            return {
                "name": new_folder_path,
                "bucket": bucket,
                "id": f"{bucket}/{new_folder_path}",
                "kind": "storage#object",
                "mediaLink": blob.media_link,
                "selfLink": blob.self_link,
                "generation": blob.generation,
                "metageneration": blob.metageneration,
                "contentType": "application/x-www-form-urlencoded;charset=UTF-8",
                "timeCreated": (
                    blob.time_created.isoformat() if blob.time_created else ""
                ),
                "updated": blob.updated.isoformat() if blob.updated else "",
                "storageClass": blob.storage_class,
                "size": "0",
                "md5Hash": blob.md5_hash,
                "etag": blob.etag,
            }
        except Exception as e:
            self.log.exception("Error creating folder")
            return {"error": str(e)}

    async def delete_file(self, bucket, path):
        try:
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)

            # Get the bucket
            bucket_obj = client.bucket(bucket)

            # Check if the blob exists
            blob = bucket_obj.blob(path)
            print("blob", blob)
            if not blob.exists():
                return {"error": "File not found", "status": 404}

            # Attempt to delete the blob
            try:
                blob.delete()
                return {"success": True}
            except Exception as e:
                # Check if it's a folder/bucket deletion attempt
                if path.endswith("/") or path == "":
                    return {
                        "error": "Deleting Folder/Bucket is not allowed",
                        "status": 404,
                    }
                else:
                    # For other deletion errors
                    return {"error": str(e), "status": 500}

        except Exception as e:
            self.log.exception(f"Error deleting file {path}")
            return {"error": str(e), "status": 500}
