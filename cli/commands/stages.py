# Copyright 2018 Google Inc
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

import os
import random
import string

import click

from cli.utils import constants
from cli.utils import settings
from cli.utils import shared
from cli.utils import stage_file_template


STAGE_VERSION_1_0 = "v1.0"
STAGE_VERSION_2_0 = "v2.0"

SUPPORTED_STAGE_VERSIONS = (STAGE_VERSION_1_0, STAGE_VERSION_2_0)


def _get_regions(project_id):
  gcloud = '$GOOGLE_CLOUD_SDK/bin/gcloud --quiet'
  cmd = f'{gcloud} app describe --verbosity critical --project={project_id}'
  cmd += '| grep locationId'
  status, out, err = shared.execute_command(
      'Get App Engine region', cmd, stream_output_in_debug=False)
  if status == 0:  # App Engine app had already been deployed in some region.
    region = out.strip().split()[1]
  else:  # Get the list of available App Engine regions and prompt user.
    click.echo('     No App Engine app has been deployed yet.')
    cmd = f"{gcloud} app regions list --format='value(region)'"
    status, out, err = shared.execute_command(
        'Get available App Engine regions', cmd, stream_output_in_debug=False)
    regions = out.strip().split('\n')
    for i, region in enumerate(regions):
      click.echo(f'{i + 1}) {region}')
    i = -1
    while i < 0 or i >= len(regions):
      i = click.prompt(
          'Enter an index of the region to deploy CRMint in', type=int) - 1
    region = regions[i]
  sql_region = region if region[-1].isdigit() else f'{region}1'
  return region, sql_region


def _default_stage_context(stage_name):
  region, sql_region = _get_regions(stage_name)
  return dict(
      project_id=settings.PROJECT,
      project_region=region,
      workdir="/tmp/{}".format(stage_name),
      database_name=settings.DATABASE_NAME,
      database_region=sql_region,
      database_tier=settings.DATABASE_TIER,
      database_username=settings.DATABASE_USER,
      database_password=settings.DATABASE_PASSWORD,
      database_instance_name=settings.DATABASE_INSTANCE_NAME,
      database_public_ip=settings.DATABASE_PUBLIC_IP,
      database_backup_enabled=settings.DATABASE_BACKUP_ENABLED,
      database_ha_type=settings.DATABASE_HA_TYPE,
      database_project=settings.DATABASE_PROJECT,
      network=settings.NETWORK,
      subnet=settings.SUBNET,
      subnet_region=settings.SUBNET_REGION,
      subnet_cidr=settings.SUBNET_CIDR,
      connector=settings.CONNECTOR,
      connector_subnet=settings.CONNECTOR_SUBNET,
      connector_cidr=settings.CONNECTOR_CIDR,
      connector_min_instances=settings.CONNECTOR_MIN_INSTANCES,
      connector_max_instances=settings.CONNECTOR_MAX_INSTANCES,
      connector_machine_type=settings.CONNECTOR_MACHINE_TYPE,
      network_project=settings.NETWORK_PROJECT,
      gae_project=settings.GAE_PROJECT,
      gae_region=region,
      gae_app_title=settings.GAE_APP_TITLE,
      pubsub_verification_token=settings.PUBSUB_VERIFICATION_TOKEN,
      notification_sender_email="noreply@{}.appspotmail.com".format(stage_name))


def _create_stage_file(stage_name, context=None):
  filename = f'{stage_name}.py'
  filepath = os.path.join(constants.STAGE_DIR, filename)
  if context is None:
    context = _default_stage_context(stage_name)
  content = stage_file_template.STAGE_FILE_TEMPLATE.format(**context)
  with open(filepath, 'w+') as fp:
    fp.write(content)
  return filepath


def _detect_stage_version(stage_name):
  """
  Stage version is defined as:
    - `v1` for bash script stage definitions
    - `v2+` for python stage definitions

  Starts by checking for latest version.

  Returns:
      (version, filepath)
  """
  stage_python_filepath = shared.get_stage_file(stage_name)
  if os.path.exists(stage_python_filepath):
    stage = shared.get_stage_object(stage_name)
    stage_version = getattr(stage, "spec_version", STAGE_VERSION_2_0)
    if stage_version not in SUPPORTED_STAGE_VERSIONS:
      raise ValueError("Unsupported spec version: '%s'. "
                       "Supported versions are %s" % (
                            stage_version,
                            SUPPORTED_STAGE_VERSIONS))
    return stage_version, stage_python_filepath

  stage_bash_filepath = os.path.join(
      constants.PROJECT_DIR,
      "scripts/variables/stages",
      "%s.sh" % stage_name)
  if os.path.exists(stage_bash_filepath):
    return STAGE_VERSION_1_0, stage_bash_filepath

  raise ValueError("No stage file found for name: '%s'" % stage_name)


def _parse_old_stage_file(stage_name):
  """
  Parse old stage file content.
  """
  old_version, old_filepath = _detect_stage_version(stage_name)
  if old_version == STAGE_VERSION_1_0:
    # Loads bash env variables.
    cmd = "source %s" % old_filepath
    cmd += " && set 2>/dev/null"
    status, out, err = shared.execute_command(
        "Load bash environment variables",
        cmd,
        cwd=constants.PROJECT_DIR,
        stream_output_in_debug=False)

    # Converts these env vars to dict representation.
    old_stage = {}
    for line in out.split("\n"):
      key, _, value = line.partition("=")
      old_stage[key] = value

    return old_stage
  elif old_version == STAGE_VERSION_2_0:
    # Latest version
    return None


@click.group()
def cli():
  """Manage multiple instances of CRMint"""
  pass


@cli.command('create')
@click.option('--stage_name', default=None)
def create(stage_name):
  """Create new stage file"""
  if not stage_name:
    stage_name = shared.get_default_stage_name()

  if shared.check_stage_file(stage_name):
    click.echo(click.style("This stage name already exists. You can list "
                           "them all with: `$ crmint stages list`",
                           fg='red', bold=True))
    exit(1)

  filepath = _create_stage_file(stage_name)
  click.echo(click.style("Stage file created: %s" % filepath, fg='green'))


def _ignore_stage_file(file_name):
  IGNORED_STAGE_FILES = ["__init__.py", "__pycache__"]
  ENDS_WITH = [".pyc", ".example"]
  return file_name in IGNORED_STAGE_FILES or file_name.endswith(tuple(ENDS_WITH))


@cli.command('list')
def list_stages():
  """List your stages defined in cli/stages directory"""
  for file_name in os.listdir(constants.STAGE_DIR):
    if not _ignore_stage_file(file_name):
      click.echo(file_name[:-3])


@cli.command('migrate')
@click.option('--stage_name', default=None)
def migrate(stage_name):
  """Migrate old stage file format to the latest one"""
  if not stage_name:
    stage_name = shared.get_default_stage_name()

  try:
    old_context = _parse_old_stage_file(stage_name)
    if old_context is None:
      click.echo(click.style(
        "Already latest version detected: %s" % stage_name, fg='green'))
      exit(0)
  except ValueError as inst:
    click.echo(click.style(str(inst), fg='red', bold=True))
    exit(1)

  # Save the new stage
  # NB: we expect the variable names to be identical between old and new context
  new_stage = _default_stage_context(stage_name)
  new_stage.update(old_context)
  filepath = _create_stage_file(stage_name, new_stage)
  click.echo(click.style(
      "Successfully migrated stage file to: %s" % filepath, fg='green'))
