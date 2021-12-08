# Copyright 2020 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""CRMint's abstract worker dealing with BigQuery."""


from fnmatch import fnmatch
from google.cloud import storage
from jobs.workers.worker import Worker, WorkerException


class StorageWorker(Worker):  # pylint: disable=too-few-public-methods
  """Abstract worker class for Cloud Storage workers."""

  _client = None

  def _get_storage_client(self):
    if self._client is None:
      self._client = storage.Client()
    return self._client

  def _get_matching_blobs(self, patterned_uris):
    client = self._get_storage_client()
    blobs = []
    blob_name_patterns = {}
    for patterned_uri in patterned_uris:
      patterned_uri_split = patterned_uri.split('/')
      bucket_name = patterned_uri_split[2]
      blob_name_pattern = '/'.join(patterned_uri_split[3:])
      try:
        if blob_name_pattern not in blob_name_patterns[bucket_name]:
          blob_name_patterns[bucket_name].append(blob_name_pattern)
      except KeyError:
        blob_name_patterns[bucket_name] = [blob_name_pattern]
    for bucket_name in blob_name_patterns:
      print(f'bucket_name = {bucket_name}', flush=True)
      bucket = storage.Bucket(client, bucket_name)
      for blob in client.list_blobs(bucket):
        for blob_name_pattern in blob_name_patterns[bucket_name]:
          if fnmatch(blob.name, blob_name_pattern):
            blobs.append(blob)
            break
    return blobs

  def _get_matching_stats(self, patterned_uris):
    client = self._get_storage_client()
    stats = []
    patterns = {}
    for patterned_uri in patterned_uris:
      patterned_uri_split = patterned_uri.split('/')
      bucket = '/'.join(patterned_uri_split[2:3])
      pattern = '/'.join(patterned_uri_split[2:])
      try:
        if pattern not in patterns[bucket]:
          patterns[bucket].append(pattern)
      except KeyError:
        patterns[bucket] = [pattern]
    for bucket in patterns:
      for stat in client.list_blobs(bucket):
        for pattern in patterns[bucket]:
          if fnmatch(f'{bucket}/{stat.name}', pattern):
            stats.append(stat)
            break
    return stats

  def _delete_file(self, bucket_name, blob_name):
    client = self._get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

  def _get_uri_parts(self, uri):
    bucket = uri.split('/')[2]
    blob = uri.split('/')[3]
    return bucket, blob
