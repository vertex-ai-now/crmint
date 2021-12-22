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

"""CRMint's abstract worker dealing with Google Analytics."""
import os
from googleapiclient.discovery import build
from google.auth import compute_engine
from jobs.workers.worker import Worker, WorkerException


class GAWorker(Worker):
  """Abstract class with GA-specific methods."""

  def _ga_setup(self, v='v4'):
    credentials = compute_engine.Credentials()
    service = 'analyticsreporting' if v == 'v4' else 'analytics'
    self._ga_client = build(service, v, credentials=credentials)

  def _parse_accountid_from_propertyid(self):
    return self._params['property_id'].split('-')[1]
