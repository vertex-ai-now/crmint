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

class VertexAIToBQPredictor(Worker):
  """Worker to train a Vertex AI AutoML model using a Vertex dataset."""

  PARAMS = [
      ('vertexai_model_name', 'string', True, '', 'Vertex AI Model Name'),
      ('vertexai_batch_prediction_name', 'string', False, '',
       'Vertex AI Batch Prediction Name'),
      ('region', 'string', True, '', 'Region'),
      ('bq_project_id', 'string', True, '', 'BQ Project ID'),
      ('bq_dataset_id', 'string', True, '', 'BQ Dataset ID'),
      ('bq_table_id', 'string', True, '', 'BQ Table ID'),
  ]

  def _get_model(self, display_name):
    models = aiplatform.Model.list(
      filter = f'display_name="{display_name}"',
      order_by = "create_time desc")
    for m in models:
      return m
    return None

  def _execute_batch_prediction(self):
    aiplatform.init()
    model = self._get_model(self._params['vertexai_model_name'])
    if model is None:
      self.log_info('No model found. Please try again.')
      return
    project_id = self._params['bq_project_id']
    dataset_id = self._params['bq_dataset_id']
    table_id = self._params['bq_table_id']
    batch_prediction_name = self._params['vertexai_batch_prediction_name']
    if batch_prediction_name is None:
      batch_prediction_name = f'{project_id}.{dataset_id}.{table_id}'    
    job = aiplatform.BatchPredictionJob().create(
      job_display_name = f'{batch_prediction_name}',
      model_name = model,
      instances_format = 'bigquery',
      predictions_format = 'bigquery',
      bigquery_source = f'bq://{project_id}.{dataset_id}.{table_id}',
      biquery_destination_prefix = f'bq://{project_id}.{dataset_id}',
    )
    job.wait_for_resource_creation()
    job_client = self._get_vertexai_job_client(
      self._params['region'])
    batch_prediction_name = job.resource_name
    batch_prediction_job = self._get_batch_prediction_job(
      job_client, batch_prediction_name)
    self._wait_for_job(batch_prediction_job)
    
  def _execute(self):
    self._execute_batch_prediction()
    self.log_info('Finished successfully!')
