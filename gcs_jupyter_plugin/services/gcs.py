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
import json
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


    # gcs -- list bucket implementation
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

    # gcs -- list files implementation
    async def list_files(self, bucket , prefix):
        try:
            result = {}
            file_list = []
            subdir_list = []
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)
            blobs = client.list_blobs(bucket , prefix=prefix, delimiter="/")
            bucketObj = client.bucket(bucket)
            files = list(blobs)

            # Adding Sub-directories
            if blobs.prefixes:
                for pref in blobs.prefixes:
                    subdir_name = pref[:-1]
                    subdir_list.append(
                    {
                        "prefixes": {
                            "name": pref
                        }
                    }
                )
            
            # Adding Files
            for file in files:
                file_list.append(
                    {
                        "items": {
                            "name": file.name,
                            "timeCreated": file.time_created.isoformat() if file.time_created else "",
                            "updated": file.updated.isoformat() if file.updated else "",
                            "size": file.size,
                            "content_type": file.content_type,
                        }
                    }
                )
            
            result["prefixes"] = subdir_list
            result["files"] = file_list
            return result
        
        except Exception as e:
            self.log.exception(f"Error listing files: {e}")
            return [] #Return empty list on error.

    async def get_file(self, bucket, file_path , format):
        try:
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)
            bucket = client.bucket(bucket)
            blob = bucket.blob(file_path)

            if format == 'base64':
                return base64.b64encode(blob.download_as_bytes()).decode('utf-8')
            elif format == 'json':
                file_content = blob.download_as_text()
                return json.loads(file_content)
            else:
                return blob.download_as_text()

        except Exception as e:
            self.log.exception(f"Error getting file: {e}")
            return [] #Return empty list on error.