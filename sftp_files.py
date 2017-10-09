cdss
# coding: utf-8


import pysftp
import sys
import imp
import base64
from os import listdir
from os.path import isfile, join
import os
import re
import datetime
from EmailerLogger import *
from ConfigUtils import *
import time

def make_email_msgs(fileList, configItems):
	jobStatus = True
	if len(fileList) < 2:
		jobStatus = False
	msg_dict = {'msg': None, 'subject_line': None}
	fileListStr = "\n".join(fileList)
	if jobStatus:
		msg_dict['subject_line'] = "SUCCESS: " + configItems['job_name'] + " succeeded"
		msg_dict['msg']= "Job Success: " + configItems['job_name'] + " succeeded \n\n\n" + "The following files are availible on the sftp: " +  fileListStr
	else:
		msg_dict['subject_line'] = "FAILED: " + configItems['job_name'] + " FAILED"
		msg_dict['msg']= "JOB FAILED: " + configItems['job_name'] + " FAILED! \n\n\n" + fileListStr + " did NOT transfer to sftp. \n\n Please go check the logs"
	return msg_dict

config_inputdir = '/Users/j9/Desktop/stuff/sewercalls311/'
fieldConfigFile = 'fieldConfig.yaml'


cI =  ConfigItems(config_inputdir ,fieldConfigFile  )
configItems = cI.getConfigs()

lte = logETLLoad(config_inputdir, configItems)
sftp_login_config = configItems['sftp_login_config']

lg = pyLogger(configItems)
lg.setConfig()

#load custom classes for handling yaml files
Klass = imp.load_source('Configs', '/home/ubuntu/hub_datawrangle/pydev/Headless_GetData.py')


cf =  Klass.Configs(sftp_login_config)
username, password = cf.getConfigs()
print username
config_itemsList = ['sftp_url', 'file_path', 'remote_put_path']
config_items = cf.getOtherConfigs(config_itemsList)
sftp_url = config_items['sftp_url']
file_path = config_items['file_path']
remote_put_path = config_items['remote_put_path']
print sftp_url, file_path, remote_put_path



regex = str(datetime.date.today())+ "*"
regex =  "[a-zA-Z0-9_]*" + regex.replace("-", '')
regex = re.compile(regex )
#regex = re.compile(regex)
fileList = [f for f in listdir(file_path) if isfile(join(file_path, f)) and re.match("clients", f)]
fileList = [f for f in fileList if re.match(regex, f)]
fileList = [f for f in fileList if "csv" not in f]
fileList = [f.replace(" ", "_") for f in fileList]
print fileList

for fn in fileList:
    fn = file_path+ fn
    with pysftp.Connection(sftp_url, username=username, password=base64.b64decode(password) ) as sftp:
        if os.path.isfile(fn):
            sftp.chdir('HSA_Vets')
            print "uploading file: " + fn + " At " + str(datetime.date.today())
            sftp.put(fn)
print 'Upload done.'

msg_dict = make_email_msgs(fileList, configItems)

today = " for " +str(time.strftime("%m/%d/%Y"))

msg = lte.sendJobStatusEmail(msg_dict['subject_line'] + today, msg_dict['msg'])


fL = [f for f in listdir(file_path) if isfile(join(file_path, f)) and re.match("clients", f)]
for fn in fL :
    print "removing file: " + fn
    if os.path.isfile(file_path+fn):
        os.remove(file_path+fn)


