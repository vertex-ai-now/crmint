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

from google.cloud import aiplatform
from jobs.workers.worker import Worker, WorkerException

class VertexAIImporter(Worker):
  """Worker to export a BigQuery table to a Vertex AI dataset."""

  PARAMS = [
      ('bq_project_id', 'string', True, '', 'BQ Project ID'),
      ('bq_dataset_id', 'string', True, '', 'BQ Dataset ID'),
      ('bq_table_id', 'string', True, '', 'BQ Table ID'),
      ('bq_dataset_location', 'string', True, '', 'BQ Dataset Location'),
  ]

  def _execute(self):
    aiplatform.init(
      project=self._params['bq_project_id'],
      location=self._params['bq_dataset_location'])
    project_id = self._params['bq_project_id']
    dataset_id = self._params['bq_dataset_id']
    table_id = self._params['bq_table_id']
    dataset = aiplatform.TabularDataset.create(
      display_name=f'{project_id}.{dataset_id}.{table_id}',
      bq_source=f'bq://{project_id}.{dataset_id}.{table_id}')
    dataset.wait()
    self.log_info(f'Dataset created: {dataset.resource_name}')
