
import click
import logging
import os

from taca.server_status import server_status as status
from taca.utils.config import CONFIG

@click.group()
def server_status():
	""" Monitor server status """
	if not CONFIG.get('server_status', ''):
		raise RuntimeError("Configuration missing required entries: server_status")


# server status subcommands
@server_status.command()
@click.option('--gdocs', is_flag=True, default=True, help="Update the google docs")
@click.option('--statusdb', is_flag=True, default=False, help="Update the statusdb")
@click.option('--credentials', type=click.Path(exists=True), default=os.path.join(os.path.dirname(__file__), 'gdocs_credentials.json'),
				 help='Path to google credentials file')
def nases(credentials, gdocs, statusdb):
	""" Checks the available space on all the nases
	"""
	disk_space = status.get_disk_space()
	if gdocs:
		status.update_google_docs(disk_space, credentials)
	if statusdb:
		status.update_status_db(disk_space)

#  must be run on uppmax, as no passwordless ssh to uppmax servers
@server_status.command()
@click.option('--disk-quota', is_flag=True, default=True, help="Check the available space on the disks")
@click.option('--cpu-hours', is_flag=True, default=True, help="Check the usage of CPU hours")
def uppmax(disk_quota, cpu_hours):
	"""
	Checks the quotas and cpu hours on the uppmax servers
	"""
	# print cpu_hours, disk_quota
	pass