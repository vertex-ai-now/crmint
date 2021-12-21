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

class VertexAIPredictor(Worker):
  """Worker to train a Vertex AI AutoML model using a Vertex dataset."""

  PARAMS = [
      ('vertexai_model_name', 'string', True, '', 'Vertex AI Model Name'),
      ('vertexai_batch_prediction_name', 'string', True, '',
       'Vertex AI Batch Prediction Name'),
      ('bq_project_id', 'string', True, '', 'BQ Project ID'),
      ('bq_dataset_id', 'string', True, '', 'BQ Dataset ID'),
      ('bq_table_id', 'string', True, '', 'BQ Table ID'),
      ('bq_dataset_location', 'string', True, '', 'BQ Dataset Location'),
  ]

  def _get_model(self):
    display_name = self._params['vertexai_model_name']
    models = aiplatform.Model.list(
      filter = f'display_name="{display_name}"',
      order_by = "create_time desc")
    for m in models:
      return m
    return None

  def _create_batch_prediction_job(self, model):
    project_id = self._params['bq_project_id']
    dataset_id = self._params['bq_dataset_id']
    table_id = self._params['bq_table_id']
    batch_prediction_name = self._params['vertexai_batch_prediction_name']
    aiplatform.BatchPredictionJob.create(
      job_display_name = f'{batch_prediction_name}',
      model_name = model,
      instances_format = 'bigquery',
      predictions_format = 'bigquery',
      bigquery_source = f'bq://{project_id}.{dataset_id}.{table_id}'
    )

  def _execute(self):
    aiplatform.init(
      project=self._params['bq_project_id'],
      location=self._params['bq_dataset_location'])
    model = self._get_model()
    if model is not None:
      self._create_batch_prediction_job(model)
      self.log_info('Created Vertex AI batch prediction job.')
    else:
      self.log_info('No model found. Please try again.')
