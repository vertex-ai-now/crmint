# Copyright 2021 Google Inc
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


STAGE_FILE_TEMPLATE = """
#
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

###
# Variables for stage
###

# Project config
project_id = "{project_id}"
project_region = "{project_region}"

# Directory on your space to deploy
# NB: if kept empty this will defaults to /tmp/<project_id_gae>
workdir = "{workdir}"

# Database config
database_name = "{database_name}"
database_username = "{database_username}"
database_password = "{database_password}"
database_instance_name = "{database_instance_name}"
database_disable_public_ip = "{database_public_ip}"
database_backup_enabled = "{database_backup_enabled}"
database_ha_type = "{database_ha_type}"
database_region = "{database_region}"
database_tier = "{database_tier}"
database_project = "{database_project}"
database_region = "{database_region}"

# Sender email for notifications
notification_sender_email = "{notification_sender_email}"

# AppEngine config
gae_app_title = "{gae_app_title}"
gae_project = "{gae_project}"
gae_region = "{gae_region}"

# Enable flag for looking of pipelines on other stages
# Options: True, False
enabled_stages = False

# Network configuration
network = "{network}"
subnet = "{subnet}"
subnet_region = "{subnet_region}"
subnet_cidr = "{subnet_cidr}"
connector = "{connector}"
connector_subnet = "{connector_subnet}"
connector_cidr = "{connector_cidr}"
connector_min_instances = "{connector_min_instances}"
connector_max_instances = "{connector_max_instances}"
connector_machine_type = "{connector_machine_type}"
network_project = "{network_project}"

""".strip()
