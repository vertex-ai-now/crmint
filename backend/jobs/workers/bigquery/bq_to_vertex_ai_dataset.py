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

class BQToVertexAIDataset(Worker):
  """Worker to export a BigQuery table to a Vertex AI dataset."""

  PARAMS = [
      ('bq_project_id', 'string', True, '', 'BQ Project ID'),
      ('bq_dataset_id', 'string', True, '', 'BQ Dataset ID'),
      ('bq_table_id', 'string', True, '', 'BQ Table ID'),
      ('bq_dataset_location', 'string', True, '', 'BQ Dataset Location'),
      ('vertex_ai_dataset_name', 'string', False, '', 'Vertex AI Dataset Name')
  ]

  aiplatform.init()

  def _create_dataset(self, display_name, project_id, dataset_id, table_id):
    dataset = aiplatform.TabularDataset.create(
      display_name=display_name,
      bq_source=f'bq://{project_id}.{dataset_id}.{table_id}')
    dataset.wait()
    return dataset.resource_name

  def _execute(self):
    project_id = self._params['bq_project_id']
    dataset_id = self._params['bq_dataset_id']
    table_id = self._params['bq_table_id']
    if not self._params['vertex_ai_dataset_name']:
      display_name = f'{project_id}.{dataset_id}.{table_id}'
    else:
      display_name = self._params['vertex_ai_dataset_name']
    resource_name = self._create_dataset(
      display_name, project_id, dataset_id, table_id)
    self.log_info(f'Dataset created: {resource_name}')
