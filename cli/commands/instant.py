import os
import click
from cli.commands import cloud
from cli.commands import pipelines
from cli.utils import shared

@click.group()
def cli():
  """Manage your CRMint instance on GCP."""

@cli.command('bqml')
@click.option('--stage_name', type=str, default=None)
@click.option('--debug/--no-debug', default=False)
def bqml(stage_name, debug):
  """Generate BQML pipelines."""
  msg = click.style(" ______  __    __   ______  ________   ______   __    __  ________        _______    ______   __       __  __\n", fg='bright_blue')       
  msg += click.style("|      \|  \  |  \ /      \|        \ /      \ |  \  |  \|        \      |       \  /      \ |  \     /  \|  \\\n", fg='bright_blue')      
  msg += click.style("  \$$$$$$| $$\ | $$|  $$$$$$\\$$$$$$$$|  $$$$$$\| $$\ | $$ \$$$$$$$$      | $$$$$$$\|  $$$$$$\| $$\   /  $$| $$\n", fg='bright_blue')      
  msg += click.style("  | $$  | $$$\| $$| $$___\$$  | $$   | $$__| $$| $$$\| $$   | $$         | $$__/ $$| $$  | $$| $$$\ /  $$$| $$\n", fg='bright_red')
  msg += click.style("  | $$  | $$$$\ $$ \$$    \   | $$   | $$    $$| $$$$\ $$   | $$         | $$    $$| $$  | $$| $$$$\  $$$$| $$\n", fg='bright_red')      
  msg += click.style("  | $$  | $$\$$ $$ _\$$$$$$\  | $$   | $$$$$$$$| $$\$$ $$   | $$         | $$$$$$$\| $$ _| $$| $$\$$ $$ $$| $$\n", fg='bright_yellow')      
  msg += click.style(" _| $$_ | $$ \$$$$|  \__| $$  | $$   | $$  | $$| $$ \$$$$   | $$         | $$__/ $$| $$/ \ $$| $$ \$$$| $$| $$_____\n", fg='bright_yellow')
  msg += click.style("|   $$ \| $$  \$$$ \$$    $$  | $$   | $$  | $$| $$  \$$$   | $$         | $$    $$ \$$ $$ $$| $$  \$ | $$| $$     \\\n", fg='bright_green')
  msg += click.style(" \$$$$$$ \$$   \$$  \$$$$$$    \$$    \$$   \$$ \$$   \$$    \$$          \$$$$$$$   \$$$$$$\ \$$      \$$ \$$$$$$$$\n", fg='bright_green')
  msg += click.style("                                                                                        \$$$", fg='bright_green')
  click.echo(msg)                                                                                                                    
  stage_name, stage = cloud.fetch_stage_or_default(stage_name, debug=debug)
  stage = shared.before_hook(stage, stage_name)
  platforms = ['GA4', 'Universal Analytics']
  click.echo(
    'Instant BQML is available for both GA4 & Universal Analytics\n'
    'Google Analytics property types.\n'
    '--------------------------------------------')
  for i, p in enumerate(platforms):
    click.echo(f'{i + 1}) {p}')
  ind = click.prompt(
    'Enter the index for the Google Analytics property type', type=int) - 1
  platform = platforms[ind]
  if platform == 'GA4':
    training_file, prediction_file = pipelines._get_ga4_config(stage)
  if platform == 'Universal Analytics':
    training_file, prediction_file = pipelines._get_ua_config(stage)
  local_db_uri = stage.local_db_uri
  env_vars = f'DATABASE_URI="{local_db_uri}" FLASK_APP=controller_app.py'
  cloud.install_required_packages(stage)
  cloud.display_workdir(stage)
  cloud.copy_src_to_workdir(stage)
  cloud.download_cloud_sql_proxy(stage)
  cloud.start_cloud_sql_proxy(stage)
  cloud.install_python_packages(stage)
  cmd_workdir = os.path.join(stage.workdir, 'backend')
  cmd = (
      ' . .venv_controller/bin/activate &&'
      f' {env_vars} python -m flask import-pipelines {training_file} &&'
      f' {env_vars} python -m flask import-pipelines {prediction_file}'
  )
  shared.execute_command(
      'Importing training & prediction pipelines', cmd,
      cwd=cmd_workdir, debug=debug)
  cloud.stop_cloud_sql_proxy(stage)
  
if __name__ == '__main__':
  cli()
