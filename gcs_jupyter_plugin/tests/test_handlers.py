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


import asyncio
import unittest
from unittest import mock
from google.cloud import storage
import datetime
import json
import base64

from gcs_jupyter_plugin.services.gcs import Client


class TestGCSClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Mock logging
        self.log = mock.MagicMock()

        # Mock client session
        self.client_session = mock.MagicMock()

        # Valid credentials
        self.valid_credentials = {
            "access_token": "fake-token",
            "project_id": "test-project",
            "region_id": "us-central1"
        }

        # Set up the client with valid credentials
        self.client = Client(self.valid_credentials, self.log, self.client_session)

    def test_init_missing_credentials(self):
        """Test initialization with missing credentials"""
        invalid_credentials = [
            {"project_id": "test-project", "region_id": "us-central1"},  # Missing access_token
            {"access_token": "fake-token", "region_id": "us-central1"},  # Missing project_id
            {"access_token": "fake-token", "project_id": "test-project"}  # Missing region_id
        ]

        for creds in invalid_credentials:
            with self.assertRaises(ValueError):
                Client(creds, self.log, self.client_session)

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    def test_list_buckets_success(self, mock_credentials, mock_storage_client):
        """Test successful bucket listing"""
        # Set up bucket mock objects
        bucket1 = mock.MagicMock()
        bucket1.name = "bucket1"
        bucket1.updated = datetime.datetime(2023, 1, 1, 12, 0, 0)

        bucket2 = mock.MagicMock()
        bucket2.name = "bucket2"
        bucket2.updated = None
        mock_client_instance = mock_storage_client.return_value
        mock_client_instance.list_buckets.return_value = [bucket1, bucket2]
        result = asyncio.run(self.client.list_buckets())

        mock_credentials.assert_called_once_with("fake-token")

        mock_storage_client.assert_called_once_with(
            project="test-project",
            credentials=mock_credentials.return_value
        )

        mock_client_instance.list_buckets.assert_called()

        expected = [
            {"items": {"name": "bucket1", "updated": "2023-01-01T12:00:00"}},
            {"items": {"name": "bucket2", "updated": ""}}
        ]
        self.assertEqual(result, expected)

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    def test_list_buckets_with_prefix(self, mock_credentials, mock_storage_client):
        """Test bucket listing with prefix filter"""
        bucket = mock.MagicMock()
        bucket.name = "test-bucket"
        bucket.updated = datetime.datetime(2023, 1, 1, 12, 0, 0)

        mock_client_instance = mock_storage_client.return_value
        mock_client_instance.list_buckets.return_value = [bucket]

        result = asyncio.run(self.client.list_buckets(prefix="test"))

        mock_client_instance.list_buckets.assert_called_with(prefix="test")

        expected = [
            {"items": {"name": "test-bucket", "updated": "2023-01-01T12:00:00"}}
        ]
        self.assertEqual(result, expected)

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_list_files_empty(self, mock_credentials, mock_storage_client):
        """Test listing files when the bucket is empty or prefix yields no results"""
        mock_blobs = mock.MagicMock()
        mock_blobs.prefixes = []
        mock_blobs.__iter__.return_value = []
        mock_storage_client.return_value.list_blobs.return_value = mock_blobs

        result = await self.client.list_files("test-bucket", "prefix/")
        self.assertEqual(result, {"prefixes": [], "files": []})
        mock_storage_client.return_value.list_blobs.assert_called_once_with("test-bucket", prefix="prefix/", delimiter="/")

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_list_files_with_files_only(self, mock_credentials, mock_storage_client):
        """Test listing files with only files, no subdirectories"""
        mock_blobs = mock.MagicMock()
        mock_blobs.prefixes = []
        file1_mock = mock.MagicMock()
        file1_mock.name = "file1.txt"
        file1_mock.time_created = datetime.datetime(2023, 1, 2)
        file1_mock.updated = datetime.datetime(2023, 1, 3)
        file1_mock.size = 100
        file1_mock.content_type = "text/plain"

        data_csv_mock = mock.MagicMock()
        data_csv_mock.name = "data.csv"
        data_csv_mock.time_created = datetime.datetime(2023, 1, 5)
        data_csv_mock.updated = datetime.datetime(2023, 1, 5)
        data_csv_mock.size = 200
        data_csv_mock.content_type = "text/csv"

        mock_blobs.__iter__.return_value = [file1_mock, data_csv_mock]
        mock_storage_client.return_value.list_blobs.return_value = mock_blobs

        result = await self.client.list_files("test-bucket", "")

        expected = {
            "prefixes": [],
            "files": [
                {"items": {"name": "file1.txt", "timeCreated": "2023-01-02T00:00:00", "updated": "2023-01-03T00:00:00", "size": 100, "content_type": "text/plain"}},
                {"items": {"name": "data.csv", "timeCreated": "2023-01-05T00:00:00", "updated": "2023-01-05T00:00:00", "size": 200, "content_type": "text/csv"}},
            ]
        }
        self.assertEqual(result, expected)

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_list_files_with_prefixes_only(self, mock_credentials, mock_storage_client):
        """Test listing files with only subdirectories, no files (explicit mock objects - FIXED)"""
        mock_blobs_initial = mock.MagicMock()
        mock_blobs_initial.prefixes = ["folder1/","folder2/"]
        mock_blobs_initial.__iter__.return_value = []

        folder1_item_mock = mock.MagicMock()
        folder1_item_mock.name = "folder1/item.txt"
        folder1_item_mock.updated = datetime.datetime(2023, 1, 10)

        folder2_item_mock = mock.MagicMock()
        folder2_item_mock.name = "folder2/item.txt"
        folder2_item_mock.updated = None

        mock_blobs_folder1 = mock.MagicMock()
        mock_blobs_folder1.__iter__.return_value = [folder1_item_mock]

        mock_blobs_folder2 = mock.MagicMock()
        mock_blobs_folder2.__iter__.return_value = [folder2_item_mock]

        mock_storage_client.return_value.list_blobs.side_effect = [
            mock_blobs_initial,
            mock_blobs_folder1,
            mock_blobs_folder2,
        ]
        
        result = await self.client.list_files("test-bucket", "")
        expected = {
            "prefixes": [
                {"prefixes": {"name": "folder1/", "updatedAt": "2023-01-10T00:00:00"}},
                {"prefixes": {"name": "folder2/", "updatedAt": ""}},
            ],
            "files": []
        }
        
        self.assertEqual(result, expected)

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_list_files_with_files_and_prefixes(self, mock_credentials, mock_storage_client):
        """Test listing files with both files and subdirectories"""
        mock_blobs = mock.MagicMock()
        mock_blobs.prefixes = []
        file1_mock = mock.MagicMock()
        file1_mock.name = "file1.txt"
        file1_mock.time_created = datetime.datetime(2023, 1, 2)
        file1_mock.updated = datetime.datetime(2023, 1, 3)
        file1_mock.size = 100
        file1_mock.content_type = "text/plain"

        data_csv_mock = mock.MagicMock()
        data_csv_mock.name = "data.csv"
        data_csv_mock.time_created = datetime.datetime(2023, 1, 5)
        data_csv_mock.updated = datetime.datetime(2023, 1, 5)
        data_csv_mock.size = 200
        data_csv_mock.content_type = "text/csv"

        mock_blobs_initial = mock.MagicMock()
        mock_blobs_initial.prefixes = ["folder1/"]
        mock_blobs_initial.__iter__.return_value = [file1_mock, data_csv_mock]

        folder1_item_mock = mock.MagicMock()
        folder1_item_mock.name = "folder1/item.txt"
        folder1_item_mock.updated = datetime.datetime(2023, 1, 10)

        mock_blobs_folder1 = mock.MagicMock()
        mock_blobs_folder1.__iter__.return_value = [folder1_item_mock]

        mock_storage_client.return_value.list_blobs.side_effect = [
            mock_blobs_initial,
            mock_blobs_folder1,
        ]
        result = await self.client.list_files("test-bucket", "")

        expected = {
            "prefixes": [
                {"prefixes": {"name": "folder1/", "updatedAt": "2023-01-10T00:00:00"}},
            ],
            "files": [
                {"items": {"name": "file1.txt", "timeCreated": "2023-01-02T00:00:00", "updated": "2023-01-03T00:00:00", "size": 100, "content_type": "text/plain"}},
                {"items": {"name": "data.csv", "timeCreated": "2023-01-05T00:00:00", "updated": "2023-01-05T00:00:00", "size": 200, "content_type": "text/csv"}},
            ]
        }
        self.assertEqual(result, expected)

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_list_files_exception(self, mock_credentials, mock_storage_client):
        """Test list_files when an exception occurs"""
        mock_storage_client.return_value.list_blobs.side_effect = Exception("List files error")
        result = await self.client.list_files("test-bucket", "prefix/")
        self.assertEqual(result, [])
        self.log.exception.assert_called_once()


    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_create_folder_success(self, mock_credentials, mock_storage_client):
        """Test successful creation of a folder"""
        bucket_name = "test-bucket"
        path = "parent/"
        folder_name = "new_folder"
        expected_folder_path = "parent//new_folder/"
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.media_link = "test_media_link"
        mock_blob.self_link = "test_self_link"
        mock_blob.generation = "123"
        mock_blob.metageneration = "456"
        mock_blob.time_created = datetime.datetime.now()
        mock_blob.updated = datetime.datetime.now()
        mock_blob.storage_class = "STANDARD"
        mock_blob.size = 0
        mock_blob.md5_hash = "test_md5"
        mock_blob.etag = "test_etag"

        result = await self.client.create_folder(bucket_name, path, folder_name)

        mock_storage_client.assert_called_once_with(
            project="test-project", credentials=mock_credentials.return_value
        )
        mock_bucket.blob.assert_called_once_with(expected_folder_path)
        mock_blob.upload_from_string.assert_called_once_with("")
        self.assertEqual(result["name"], expected_folder_path)
        self.assertEqual(result["bucket"], bucket_name)
        self.assertEqual(result["size"], "0")
        self.assertEqual(result["contentType"], "application/x-www-form-urlencoded;charset=UTF-8")

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_create_folder_root_path(self, mock_credentials, mock_storage_client):
        """Test creating a folder at the root level"""
        bucket_name = "test-bucket"
        path = ""
        folder_name = "new_folder"
        expected_folder_path = "new_folder/"
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.upload_from_string.return_value = None

        await self.client.create_folder(bucket_name, path, folder_name)

        mock_bucket.blob.assert_called_once_with(expected_folder_path)
        mock_blob.upload_from_string.assert_called_once_with("")

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_create_folder_exception(self, mock_credentials, mock_storage_client):
        """Test create_folder when an exception occurs"""
        mock_storage_client.return_value.bucket.side_effect = Exception("GCS error")

        result = await self.client.create_folder("test-bucket", "path/", "folder")
        self.assertIn("error", result)
        self.assertEqual(result["error"], "GCS error")
        self.log.exception.assert_called_once()

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_delete_file_success(self, mock_credentials, mock_storage_client):
        """Test successful deletion of a file"""
        bucket_name = "test-bucket"
        file_path = "path/to/file.txt"
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.exists.return_value = True

        result = await self.client.delete_file(bucket_name, file_path)

        mock_bucket.blob.assert_called_once_with(file_path)
        mock_blob.delete.assert_called_once()
        self.assertEqual(result, {"success": True})

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_delete_file_not_found(self, mock_credentials, mock_storage_client):
        """Test deleting a file that does not exist"""
        bucket_name = "test-bucket"
        file_path = "nonexistent/file.txt"
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.exists.return_value = False

        result = await self.client.delete_file(bucket_name, file_path)

        self.assertEqual(result, {"error": "File not found", "status": 404})
        mock_blob.delete.assert_not_called()

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_delete_file_folder_attempt(self, mock_credentials, mock_storage_client):
        """Test attempting to delete a folder (not allowed)"""
        bucket_name = "test-bucket"
        folder_path = "folder/"
        result = await self.client.delete_file(bucket_name, folder_path)
        self.assertEqual(result, {"error": "Deleting Folder/Bucket is not allowed", "status": 404})

        result_root = await self.client.delete_file(bucket_name, "")
        self.assertEqual(result_root, {"error": "Deleting Folder/Bucket is not allowed", "status": 404})

        mock_storage_client.return_value.bucket.return_value.blob.assert_not_called()

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_delete_file_exception(self, mock_credentials, mock_storage_client):
        """Test delete_file when an exception occurs during deletion"""
        bucket_name = "test-bucket"
        file_path = "path/to/file.txt"
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.exists.return_value = True
        mock_blob.delete.side_effect = Exception("Delete error")

        result = await self.client.delete_file(bucket_name, file_path)

        self.assertIn("error", result)
        self.assertEqual(result["error"], "Delete error")
        self.assertEqual(result["status"], 500)
        self.log.exception.assert_called_once()

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_rename_file_success(self, mock_credentials, mock_storage_client):
        """Test successful renaming of a file"""
        bucket_name = "test-bucket"
        blob_name = "old/file.txt"
        new_name = "new/file.txt"
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.exists.return_value = True
        mock_new_blob = mock_bucket.rename_blob.return_value
        mock_new_blob.name = new_name

        result = await self.client.rename_file(bucket_name, blob_name, new_name)

        mock_bucket.blob.assert_called_once_with(blob_name)
        mock_bucket.rename_blob.assert_called_once_with(mock_blob, new_name)
        self.assertEqual(result, {"name": new_name, "bucket": bucket_name, "success": True})

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_rename_file_not_found(self, mock_credentials, mock_storage_client):
        """Test renaming a file that does not exist"""
        bucket_name = "test-bucket"
        blob_name = "old/file.txt"
        new_name = "new/file.txt"
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.exists.return_value = False

        result = await self.client.rename_file(bucket_name, blob_name, new_name)

        self.assertEqual(result, {"error": f"Source file {blob_name} not found", "status": 404})
        mock_bucket.rename_blob.assert_not_called()

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_rename_file_exception(self, mock_credentials, mock_storage_client):
        """Test rename_file when an exception occurs"""
        bucket_name = "test-bucket"
        blob_name = "old/file.txt"
        new_name = "new/file.txt"
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.exists.return_value = True
        mock_bucket.rename_blob.side_effect = Exception("Rename error")

        result = await self.client.rename_file(bucket_name, blob_name, new_name)

        self.assertIn("error", result)
        self.assertEqual(result["error"], "Rename error")
        self.assertEqual(result["status"], 500)
        self.log.exception.assert_called_once()

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_save_content_string(self, mock_credentials, mock_storage_client):
        """Test saving string content to a file"""
        bucket_name = "test-bucket"
        destination_blob_name = "path/to/new_file.txt"
        content = "This is some text content."
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.size = len(content)
        mock_blob.content_type = "media"
        mock_blob.time_created = datetime.datetime.now()
        mock_blob.updated = datetime.datetime.now()

        result = await self.client.save_content(bucket_name, destination_blob_name, content)

        mock_bucket.blob.assert_called_once_with(destination_blob_name)
        mock_blob.upload_from_string.assert_called_once_with(content, content_type="media")
        self.assertEqual(result["name"], destination_blob_name)
        self.assertEqual(result["bucket"], bucket_name)
        self.assertEqual(result["size"], len(content))
        self.assertEqual(result["contentType"], "media")
        self.assertTrue(result["success"])

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_save_content_json(self, mock_credentials, mock_storage_client):
        """Test saving JSON content to a file"""
        bucket_name = "test-bucket"
        destination_blob_name = "data/config.json"
        content = {"key": "value", "number": 123}
        json_content = json.dumps(content)
        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.size = len(json_content)
        mock_blob.content_type = "media"
        mock_blob.time_created = datetime.datetime.now()
        mock_blob.updated = datetime.datetime.now()

        result = await self.client.save_content(bucket_name, destination_blob_name, content)

        mock_bucket.blob.assert_called_once_with(destination_blob_name)
        mock_blob.upload_from_string.assert_called_once_with(json_content, content_type="media")
        self.assertEqual(result["name"], destination_blob_name)
        self.assertEqual(result["bucket"], bucket_name)
        self.assertEqual(result["size"], len(json_content))
        self.assertEqual(result["contentType"], "media")
        self.assertTrue(result["success"])

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_save_content_exception(self, mock_credentials, mock_storage_client):
        """Test save_content when an exception occurs"""
        mock_storage_client.return_value.bucket.return_value.blob.return_value.upload_from_string.side_effect = Exception("Upload error")

        result = await self.client.save_content("test-bucket", "file.txt", "content")
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Upload error")
        self.assertEqual(result["status"], 500)
        self.log.exception.assert_called_once()

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_get_file_base64(self, mock_credentials, mock_storage_client):
        """Test getting a file in base64 format."""
        bucket_name = "test-bucket"
        file_path = "test/file.txt"
        file_content = b"This is a test file."
        expected_base64 = base64.b64encode(file_content).decode('utf-8')

        mock_blob = mock.MagicMock()
        mock_blob.download_as_bytes.return_value = file_content
        mock_bucket = mock.MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        result = await self.client.get_file(bucket_name, file_path, format='base64')

        mock_credentials.assert_called_once_with(self.client._access_token)
        mock_storage_client.assert_called_once_with(project=self.client.project_id, credentials=mock.ANY)
        mock_storage_client.return_value.bucket.assert_called_once_with(bucket_name)
        mock_bucket.blob.assert_called_once_with(file_path)
        mock_blob.download_as_bytes.assert_called_once()
        self.assertEqual(result, expected_base64)

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_get_file_json(self, mock_credentials, mock_storage_client):
        """Test getting a file in JSON format."""
        bucket_name = "test-bucket"
        file_path = "data/payload.json"
        file_content = '{"key": "value", "number": 123}'
        expected_json = json.loads(file_content)

        mock_blob = mock.MagicMock()
        mock_blob.download_as_text.return_value = file_content
        mock_bucket = mock.MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        result = await self.client.get_file(bucket_name, file_path, format='json')

        mock_credentials.assert_called_once_with(self.client._access_token)
        mock_storage_client.assert_called_once_with(project=self.client.project_id, credentials=mock.ANY)
        mock_storage_client.return_value.bucket.assert_called_once_with(bucket_name)
        mock_bucket.blob.assert_called_once_with(file_path)
        mock_blob.download_as_text.assert_called_once()
        self.assertEqual(result, expected_json)

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_get_file_text(self, mock_credentials, mock_storage_client):
        """Test getting a file in text format (default)."""
        bucket_name = "test-bucket"
        file_path = "logs/output.log"
        file_content = "This is a log entry.\nAnother line."

        mock_blob = mock.MagicMock()
        mock_blob.download_as_text.return_value = file_content
        mock_bucket = mock.MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        result = await self.client.get_file(bucket_name, file_path, format='text') # Explicitly using 'text'

        mock_credentials.assert_called_once_with(self.client._access_token)
        mock_storage_client.assert_called_once_with(project=self.client.project_id, credentials=mock.ANY)
        mock_storage_client.return_value.bucket.assert_called_once_with(bucket_name)
        mock_bucket.blob.assert_called_once_with(file_path)
        mock_blob.download_as_text.assert_called_once()
        self.assertEqual(result, file_content)

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_get_file_default_format(self, mock_credentials, mock_storage_client):
        """Test getting a file with the default format (text)."""
        bucket_name = "test-bucket"
        file_path = "docs/README.md"
        file_content = "# Readme\nThis is a markdown file."

        mock_blob = mock.MagicMock()
        mock_blob.download_as_text.return_value = file_content
        mock_bucket = mock.MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        result = await self.client.get_file(bucket_name, file_path, format=None) # Passing None to use default

        mock_credentials.assert_called_once_with(self.client._access_token)
        mock_storage_client.assert_called_once_with(project=self.client.project_id, credentials=mock.ANY)
        mock_storage_client.return_value.bucket.assert_called_once_with(bucket_name)
        mock_bucket.blob.assert_called_once_with(file_path)
        mock_blob.download_as_text.assert_called_once()
        self.assertEqual(result, file_content)

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('google.oauth2.credentials.Credentials')
    async def test_get_file_error(self, mock_credentials, mock_storage_client):
        """Test the error handling when getting a file fails."""
        bucket_name = "error-bucket"
        file_path = "nonexistent/file.txt"
        mock_exception = Exception("File not found")

        mock_blob = mock.MagicMock()
        mock_blob.download_as_bytes.side_effect = mock_exception
        mock_blob.download_as_text.side_effect = mock_exception
        mock_bucket = mock.MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        result = await self.client.get_file(bucket_name, file_path, format='base64')
        self.assertEqual(result, [])
        self.client.log.exception.assert_called_once_with(f"Error getting file: {mock_exception}")

        result = await self.client.get_file(bucket_name, file_path, format='json')
        self.assertEqual(result, [])
        self.client.log.exception.assert_called_with(f"Error getting file: {mock_exception}") # Called again

        result = await self.client.get_file(bucket_name, file_path, format='text')
        self.assertEqual(result, [])
        self.client.log.exception.assert_called_with(f"Error getting file: {mock_exception}") # Called again

        result = await self.client.get_file(bucket_name, file_path, format=None)
        self.assertEqual(result, [])
        self.client.log.exception.assert_called_with(f"Error getting file: {mock_exception}") # Called again