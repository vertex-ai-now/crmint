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

from datetime import datetime
from datetime import timedelta
import time
from jobs.workers.storage.storage_worker import StorageWorker


class StorageCleaner(StorageWorker):
  """Worker to delete stale files in Cloud Storage."""

  PARAMS = [
      ('file_uris', 'string_list', True, '',
       ('List of file URIs and URI patterns (e.g. gs://bucket/data.csv or '
        'gs://bucket/data_*.csv)')),
      ('expiration_days', 'number', True, 30,
       'Days to keep files since last modification'),
  ]

  def _execute(self):
    delta = timedelta(self._params['expiration_days'])
    expiration_datetime = datetime.now() - delta
    expiration_timestamp = time.mktime(expiration_datetime.timetuple())
    stats = self._get_matching_stats(self._params['file_uris'])
    for stat in stats:
      if stat.updated < expiration_timestamp:
        self._delete_file(stat.bucket.name, stat.name)
        self.log_info('gs:/%s file deleted.', stat.name)
