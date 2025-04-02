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


import json
import aiohttp
import tornado
from jupyter_server.base.handlers import APIHandler

from gcs_jupyter_plugin import credentials
from gcs_jupyter_plugin.services import gcs


class ListBucketsController(APIHandler):
    @tornado.web.authenticated
    async def get(self):
        try:
            prefix = self.get_argument("prefix")
            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                buckets = await client.list_buckets(prefix)
            self.finish(json.dumps(buckets))
        except Exception as e:
            self.log.exception("Error fetching datasets")
            self.finish({"error": str(e)})


class CreateFolderController(APIHandler):
    @tornado.web.authenticated
    async def post(self):
        try:
            data = json.loads(self.request.body)
            bucket = data.get("bucket")
            path = data.get("path", "")
            folder_name = data.get("folderName")

            if not bucket or not folder_name:
                self.set_status(400)
                self.finish({"error": "Missing required parameters"})
                return

            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                folder = await client.create_folder(bucket, path, folder_name)
            self.finish(json.dumps(folder))
        except Exception as e:
            self.log.exception("Error creating folder")
            self.set_status(500)
            self.finish({"error": str(e)})


class DeleteFileController(APIHandler):
    @tornado.web.authenticated
    async def post(self):
        try:
            data = json.loads(self.request.body)
            bucket = data.get("bucket")
            path = data.get("path")

            if not bucket or not path:
                self.set_status(400)
                self.finish({"error": "Missing required parameters"})
                return

            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                result = await client.delete_file(bucket, path)

                # Check for specific error conditions in the result
                if "error" in result:
                    if result.get("status") == 404:
                        self.finish(
                            {
                                "error": "Deleting Folder/Bucket is not allowed",
                                "status": 404,
                            }
                        )
                        return
                    else:
                        self.set_status(result.get("status", 500))
                        self.finish({"error": result.get("error")})
                        return

                # Set correct success status for delete operation
                self.set_status(204)
                self.finish()
        except Exception as e:
            self.log.exception("Error deleting file")
            self.set_status(500)
            self.finish({"error": str(e)})
