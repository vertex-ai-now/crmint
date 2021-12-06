# Copyright 2021 Google Inc. All rights reserved.
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

from random import random
import time
from apiclient.http import MediaIoBaseUpload
from jobs.workers.worker import Worker, WorkerException
from jobs.workers.ga.ga_worker import GAWorker
from jobs.workers.storage.storage_worker import StorageWorker


class GADataImporter(GAWorker, StorageWorker):
  """Imports CSV data from Cloud Storage to GA using Data Import."""

  PARAMS = [
      ('csv_uri', 'string', True, '',
       'CSV data file URI (e.g. gs://bucket/data.csv)'),
      ('property_id', 'string', True, '',
       'GA Property Tracking ID (e.g. UA-12345-3)'),
      ('dataset_id', 'string', True, '',
       'GA Dataset ID (e.g. sLj2CuBTDFy6CedBJw)'),
      ('max_uploads', 'number', False, '',
       'Maximum uploads to keep in GA Dataset (leave empty to keep all)'),
      ('delete_before', 'boolean', True, False,
       'Delete older uploads before upload'),
      ('account_id', 'string', False, '', 'GA Account ID'),
  ]

  _BUFFER_SIZE = 256 * 1024

  def _upload(self):
    storage_client = self._get_storage_client()
    bucket_name, blob_name = self._get_uri_parts(self._params['csv_uri'])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    with blob.open('rb', chunk_size=self._BUFFER_SIZE) as f:
      media = MediaIoBaseUpload(f, mimetype='application/octet-stream',
                                chunksize=self._BUFFER_SIZE, resumable=True)
      request = self._ga_client.management().uploads().uploadData(
          accountId=self._account_id,
          webPropertyId=self._params['property_id'],
          customDataSourceId=self._params['dataset_id'],
          media_body=media)
      response = None
      tries = 0
      milestone = 0
      while response is None and tries < 5:
        try:
          status, response = request.next_chunk()
        except HttpError as e:
          if e.resp.status in [404, 500, 502, 503, 504]:
            tries += 1
            delay = 5 * 2 ** (tries + random())
            self.log_warn(f'{e}, Retrying in {delay:.1f} seconds...')
            time.sleep(delay)
          else:
            raise WorkerException(e)
        else:
          tries = 0
        if status:
          progress = int(status.progress() * 100)
          if progress >= milestone:
            self.log_info('Uploaded %d%%.', int(status.progress() * 100))
            milestone += 20
      self.log_info('Upload Complete.')

  def _delete_older(self, uploads_to_keep):
    request = self._ga_client.management().uploads().list(
        accountId=self._account_id, webPropertyId=self._params['property_id'],
        customDataSourceId=self._params['dataset_id'])
    response = self.retry(request.execute)()
    uploads = sorted(response.get('items', []), key=lambda u: u['uploadTime'])
    if uploads_to_keep:
      ids_to_delete = [u['id'] for u in uploads[:-uploads_to_keep]]
    else:
      ids_to_delete = [u['id'] for u in uploads]
    if ids_to_delete:
      request = self._ga_client.management().uploads().deleteUploadData(
          accountId=self._account_id,
          webPropertyId=self._params['property_id'],
          customDataSourceId=self._params['dataset_id'],
          body={
              'customDataImportUids': ids_to_delete})
      self.retry(request.execute)()
      self.log_info('%i older upload(s) deleted.', len(ids_to_delete))

  def _execute(self):
    self._ga_setup('v3')
    if self._params['account_id']:
      self._account_id = self._params['account_id']
    else:
      self._account_id = self._parse_accountid_from_propertyid()
    self._file_name = self._params['csv_uri'].replace('gs:/', '')
    if self._params['max_uploads'] > 0 and self._params['delete_before']:
      self._delete_older(self._params['max_uploads'] - 1)
    self._upload()
    if self._params['max_uploads'] > 0 and not self._params['delete_before']:
      self._delete_older(self._params['max_uploads'])
