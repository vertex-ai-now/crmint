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

import json
from jobs.workers.worker import Worker, WorkerException
from jobs.workers.bigquery.bq_worker import BQWorker
from jobs.workers.ga.ga_worker import GAWorker

class GAAudiencesUpdater(BQWorker, GAWorker):
  """Worker to update GA audiences using values from a BQ table.
  See: https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/remarketingAudience#resource
  for more details on the required GA Audience JSON template format.
  """

  PARAMS = [
      ('property_id', 'string', True, '',
       'GA Property Tracking ID (e.g. UA-12345-3)'),
      ('bq_project_id', 'string', False, '', 'BQ Project ID'),
      ('bq_dataset_id', 'string', True, '', 'BQ Dataset ID'),
      ('bq_table_id', 'string', True, '', 'BQ Table ID'),
      ('bq_dataset_location', 'string', False, '', 'BQ Dataset Location'),
      ('template', 'text', True, '', 'GA audience JSON template'),
      ('account_id', 'string', False, '', 'GA Account ID'),
  ]

  def _infer_audiences(self):
    self._inferred_audiences = {}
    table = self._client.get_table(self._table)
    rows = self._client.list_rows(self._table, selected_fields=table.schema[:])
    fields = [f.name for f in table.schema]
    for row in rows:
      try:
        template_rendered = self._params['template'] % dict(zip(fields, row))
        audience = json.loads(template_rendered)
      except ValueError as e:
        raise WorkerException(e)
      self._inferred_audiences[audience['name']] = audience

  def _get_audiences(self):
    audiences = []
    start_index = 1
    max_results = 100
    total_results = 100
    while start_index <= total_results:
      request = self._ga_client.management().remarketingAudience().list(
          accountId=self._account_id,
          webPropertyId=self._params['property_id'],
          start_index=start_index,
          max_results=max_results)
      response = self.retry(request.execute)()
      total_results = response['totalResults']
      start_index += max_results
      audiences += response['items']
    self._current_audiences = {}
    names = self._inferred_audiences.keys()
    for audience in audiences:
      if audience['name'] in names:
        self._current_audiences[audience['name']] = audience

  def _equal(self, patch, audience):
    """Checks whether applying a patch would not change an audience.
    Args:
        patch: An object that is going to be used as a patch to update the
            audience.
        audience: An object representing audience to be patched.
    Returns:
       True if applying the patch won't change the audience, False otherwise.
    """
    dicts = [(patch, audience)]
    for d1, d2 in dicts:
      keys = d1 if isinstance(d1, dict) else range(len(d1))
      for k in keys:
        try:
          d2[k]
        except (IndexError, KeyError):
          return False
        if isinstance(d1[k], dict):
          if isinstance(d2[k], dict):
            dicts.append((d1[k], d2[k]))
          else:
            return False
        elif isinstance(d1[k], list):
          if isinstance(d2[k], list) and len(d1[k]) == len(d2[k]):
            dicts.append((d1[k], d2[k]))
          else:
            return False
        elif d1[k] != d2[k]:
          return False
    return True

  def _get_diff(self):
    """Composes lists of audiences to be created and updated in GA."""
    self._audiences_to_insert = []
    self._audiences_to_patch = {}
    for name in self._inferred_audiences:
      inferred_audience = self._inferred_audiences[name]
      if name in self._current_audiences:
        current_audience = self._current_audiences[name]
        if not self._equal(inferred_audience, current_audience):
          self._audiences_to_patch[current_audience['id']] = inferred_audience
      else:
        self._audiences_to_insert.append(inferred_audience)

  def _update_ga_audiences(self):
    """Updates and/or creates audiences in GA."""
    for audience in self._audiences_to_insert:
      request = self._ga_client.management().remarketingAudience().insert(
          accountId=self._account_id,
          webPropertyId=self._params['property_id'],
          body=audience)
      self.retry(request.execute)()
    for audience_id in self._audiences_to_patch:
      audience = self._audiences_to_patch[audience_id]
      request = self._ga_client.management().remarketingAudience().patch(
          accountId=self._account_id,
          webPropertyId=self._params['property_id'],
          remarketingAudienceId=audience_id,
          body=audience)
      self.retry(request.execute)()

  def _execute(self):
    if self._params['account_id']:
      self._account_id = self._params['account_id']
    else:
      self._account_id = self._parse_accountid_from_propertyid()
    # BQ Setup
    self._client = self._get_client()
    self._dataset = self._client.dataset(self._params['bq_dataset_id'])
    self._table = self._dataset.table(self._params['bq_table_id'])
    self._ga_setup('v3')
    self._infer_audiences()
    self._get_audiences()
    self._get_diff()
    self._update_ga_audiences()
