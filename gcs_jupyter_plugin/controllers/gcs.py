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


class ListFilesController(APIHandler):
    @tornado.web.authenticated
    async def get(self):
        try:
            prefix = self.get_argument("prefix")
            bucket = self.get_argument("bucket")
            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                files = await client.list_files(bucket,prefix)
            self.finish(json.dumps(files))
        except Exception as e:
            self.log.exception("Error fetching datasets")
            self.finish({"error": str(e)})

class LoadFileController(APIHandler):
    @tornado.web.authenticated
    async def get(self):
        try:
            bucket = self.get_argument("bucket")
            file_path = self.get_argument("path")
            format = self.get_argument("format")
            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                file = await client.get_file(bucket,file_path, format)
            self.finish(json.dumps(file))
        except Exception as e:
            self.log.exception("Error fetching datasets")
            self.finish({"error": str(e)})