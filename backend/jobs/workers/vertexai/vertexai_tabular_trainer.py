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

from jobs.workers.vertexai.vertexai_worker import VertexAIWorker

class VertexAITabularTrainer(VertexAIWorker):
  
  PARAMS = [
      ('vertexai_dataset_name', 'string', True, '', 'Vertex AI Dataset Name'),
      ('vertexai_model_name', 'string', True, '', 'Vertex AI Model Name'),
      ('prediction_type', 'string', True, '', 'Prediction Type '
       '(regression or classification)'),
      ('target_column', 'string', True, '', 'Target Column'),
      ('budget_hours', 'number', True, 1, 'Budget Hours (1 - 72)'),
  ]
  
  def _get_vertex_dataset(self):
    display_name = self._params['vertexai_dataset_name']
    dataset = aiplatform.TabularDataset.list(filter = f'display_name="{display_name}"')
    if len(dataset) > 1:
      dataset = aiplatform.TabularDataset.list(
        filter = f'display_name="{display_name}"',
        order_by = "create_time desc")
    for ds in dataset
      return ds
    return None

  def _create_automl_tabular_training_job(self):
    vertexai_model_name = self._params['vertexai_model_name']
    prediction_type = self._params['prediction_type']
    return aiplatform.AutoMLTabularTrainingJob(
      display_name=f'{vertexai_model_name}',
      optimization_prediction_type=f'{prediction_type}',
    )

  def _execute(self):
    budget_hours = self._params['budget_hours']
    target_column = self._params['target_column']
    vertexai_model_name = self._params['vertexai_model_name']
    dataset = _get_vertex_dataset(self)
    job = _create_automl_tabular_training_job(self)
    model = job.run(
      dataset = dataset,
      target_column = f'{target_column}',
      budget_milli_node_hours = f'{budget_hours} * 1000',
      model_display_name = {vertexai_model_name},
      disable_early_stopping = False,
    )
    model.wait()
    self.log_info(
      f'Created Vertex AI Model with display name {model.display_name}')
