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

"""CRMint's worker that waits for a Vertex AI job completion."""

import time
from google.cloud import aiplatform
from google.cloud.aiplatform import gapic as aip
from google.cloud.aiplatform.compat.types import pipeline_state as ps
from google.cloud.aiplatform.compat.types import job_state as js
from jobs.workers.worker import Worker, WorkerException


class VertexAIWorker(Worker):
  """Worker that polls job status and respawns itself if the job is not done."""

  def _get_vertexai_job_client(self, location):
    api_endpoint = f'{location}-aiplatform.googleapis.com'
    client_options = {'api_endpoint': api_endpoint}
    return aip.JobServiceClient(client_options=client_options)
  
  def _get_vertexai_pipeline_client(self, location):
    api_endpoint = f'{location}-aiplatform.googleapis.com'
    client_options = {'api_endpoint': api_endpoint}
    return aip.PipelineServiceClient(client_options=client_options)
  
  def _get_batch_prediction_job(self, job_client, job_name):
    return job_client.get_batch_prediction_job(name=job_name)
  
  def _get_training_pipeline(self, pipeline_client, pipeline_name):
    return pipeline_client.get_training_pipeline(name=pipeline_name)
  
  def _get_location_from_pipeline_name(self, pipeline_name):
    return pipeline_name.split('/')[3]
  
  def _get_location_from_job_name(self, job_name):
    return job_name.split('/')[3]

  def _wait_for_pipeline(self, pipeline):
    """Waits for pipeline completion and relays to VertexAIWaiter if it takes too long."""
    delay = 5
    waiting_time = 5
    time.sleep(delay)
    while pipeline.state != ps.PipelineState.PIPELINE_STATE_SUCCEEDED:
      if waiting_time > 300:  # Once 5 minutes have passed, spawn VertexAIWaiter.
        self._enqueue(
          'VertexAIWaiter', {
            'id': pipeline.name, 
            'worker_class': 'VertexAITabularTrainer'}, 
          60)
        return
      if delay < 30:
        delay = [5, 10, 15, 20, 30][int(waiting_time / 60)]
      time.sleep(delay)
      waiting_time += delay
    if pipeline.state == ps.PipelineState.PIPELINE_STATE_FAILED:
      raise WorkerException(f'Training pipeline {pipeline.name} failed.')
      
  def _wait_for_job(self, job):
    """Waits for pipeline completion and relays to VertexAIWaiter if it takes too long."""
    delay = 5
    waiting_time = 5
    time.sleep(delay)
    while job.state != js.JobState.JOB_STATE_SUCCEEDED:
      if waiting_time > 300:  # Once 5 minutes have passed, spawn VertexAIWaiter.
        self._enqueue(
          'VertexAIWaiter', {
            'id': job.name, 
            'worker_class': 'VertexAIToBQPredictor'},
          60)
        return
      if delay < 30:
        delay = [5, 10, 15, 20, 30][int(waiting_time / 60)]
      time.sleep(delay)
      waiting_time += delay
    if job.state == js.JobState.JOB_STATE_FAILED:
      raise WorkerException(f'Job {job.name} failed.')
