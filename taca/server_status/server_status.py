import subprocess
import json
import gspread
import logging
import couchdb
import datetime

from oauth2client.client import SignedJwtAssertionCredentials as GCredentials
from taca.utils.config import CONFIG

def get_disk_space():
	result = {}
	config = CONFIG['server_status']
	servers = config.get('servers', [])
	for server_url in servers.keys():
		# get path of disk
		path = servers[server_url]

		# get command
		command = "{command} {path}".format(command=config['command'], path=path)

		# if localhost, don't connect to ssh
		if server_url == "localhost":
			proc = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		else:
			# connect via ssh to server and execute the command
			proc = subprocess.Popen(['ssh', '-t', '%s@%s' %(config['user'], server_url), command],
				stdout = subprocess.PIPE,
				stderr = subprocess.PIPE)
		# read output
		output = proc.stdout.read()
		# parse output
		output = __parse_output(output)

		try:
			# remove % symbol and convert string to int
			used_space = int(output.strip().replace('%', ''))
			# how many percent available
			available_space = 100 - used_space
			result[server_url] = "{value}%".format(value=available_space)
		except:
			# sometimes it fails for whatever reason as Popen returns not what it is supposed to
			result[server_url] = 'NaN'
	return result

def __parse_output(output):
	# command = df -h /home
	# output = Filesystem            Size  Used Avail Use% Mounted on
	# /dev/mapper/VGStor-lv_illumina
    #                   24T   12T   13T  49% /srv/illumina

	output = output.strip() # remove \n in the end
	output = output.split('\n')[-1] # split by lines and take the last line
	output = output.strip() # remove spaces
	output = output.split() # split line by space symbols

	# output = ['24T', '12T', '13T', '49%', '/srv/illumina']
	for item in output: # select the item containing '%' symbol
		if '%' in item:
			return item

	return 'NaN' # if no '%' in output, return NaN

def update_google_docs(data, credentials_file):
	config = CONFIG['server_status']
	# open json file
	json_key = json.load(open(credentials_file))

	# get credentials from the file and authorize
	credentials = GCredentials(json_key['client_email'], json_key['private_key'], config['g_scope'])
	gc = gspread.authorize(credentials)
	# open google sheet
	# IMPORTANT: file must be shared with email listed in credentials
	sheet = gc.open(config['g_sheet'])

	# choose worksheet from the doc
	worksheet = sheet.get_worksheet(1)

	# update cell
	for key in data:
		cell = config['g_sheet_map'].get(key) # key = server name
		value = data.get(key)		# value = available space
		worksheet.update_acell(cell, value)


# todo: make the method universal for both uppmax and nases
def update_status_db(data):
	db_config = CONFIG.get('statusdb')
	if db_config is None:
		logging.error("'statusdb' must be present in the config file!")
		raise

	server = "http://{username}:{password}@{url}:{port}".format(
        url=db_config['url'],
        username=db_config['username'],
        password=db_config['password'],
        port=db_config['port'])
	try:
		couch = couchdb.Server(server)
	except Exception, e:
		logging.error(e.message)
		raise

	db = couch['server_status']
	logging.info('Connection established')
	for key in data.keys():
		server = {
			'name': key, # url or uppmax project
			'disk_space_used_percentage': data[key],
			'time': datetime.datetime.now()
		}
		try:
			server_id, server_rev = db.save(server)
		except Exception, e:
			logging.error(e.message)
			raise
		else:
			logging.info('{}: Server status has been updated'.format(key))


def get_uppmax_quotas():
	current_time = datetime.datetime.now()
	try:
		uq = subprocess.Popen(["/sw/uppmax/bin/uquota", "-q"], stdout=subprocess.PIPE)
	except Exception, e:
		logging.error(e.message)
		raise

	output = uq.communicate()[0]
	logging.info("Disk Usage:")
	logging.info(output)

	projects = output.split("\n/proj/")[1:]

	result = {}
	for proj in projects:
		project_dict = {"time": current_time.isoformat()}
		project = proj.strip("\n").split()
		project_dict["project"] = project[0]
		project_dict["usage (GB)"] = project[1]
		project_dict["quota limit (GB)"] = project[2]
		try:
			project_dict["over quota"] = project[3]
		except:
			pass

		result[project[0]] = project_dict
	return result



def cpu_hours():
	current_time = datetime.datetime.now()
	try:
		# script that runs on uppmax
		uq = subprocess.Popen(["/sw/uppmax/bin/projinfo", '-q'], stdout=subprocess.PIPE)
	except Exception, e:
		logging.error(e.message)
		raise

	# output is lines with the format: project_id  cpu_usage  cpu_limit
	output = uq.communicate()[0]

	logging.info("CPU Hours Usage:")
	logging.info(output)
	result = {}
	# parsing output
	for proj in output.strip().split('\n'):
		project_dict = {"time": current_time}

		# split line into a list
		project = proj.split()
		# creating objects
		project_dict["project"] = project[0]
		project_dict["cpu hours"] = project[1]
		project_dict["cpu limit"] = project[2]

	result[project[0]] = project_dict
	return result