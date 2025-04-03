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


cclass Client:
    def __init__(self, credentials, log, client_session):
        self.log = log
        if not (
            ("access_token" in credentials)
            and ("project_id" in credentials)
            and ("region_id" in credentials)
            and ("bucket" in credentials) # 
        ):
            self.log.exception("Missing required credentials")
            raise ValueError("Missing required credentials")
        self._access_token = credentials["access_token"]
        self.project_id = credentials["project_id"]
        self.region_id = credentials["region_id"]
        self.bucket_name = credentials["bucket_name"]
        self.client_session = client_session

    async def list_files(self, prefix=None):
        """Lists files in the specified bucket."""
        try:
            file_list = []
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)
            bucket = client.bucket(self.bucket_name) # bucket param
            files = bucket.list_files(prefix=prefix)

            for file in files:
                file_list.append(
                    {
                        "items": {
                            "name": file.name,
                            "updated": file.updated.isoformat() if file.updated else "",
                            "size": file.size,
                            "content_type": file.content_type,
                        }
                    }
                )
            return file_list
        except Exception as e:
            self.log.exception(f"Error listing files: {e}")
            return [] #Return empty list on error.