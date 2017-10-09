# coding: utf-8
#!/usr/bin/env python
import json
import requests
import pysftp
import sys
import base64
from os import listdir
from os.path import isfile, join
import os
import re
import datetime
from EmailerLogger import *
from ConfigUtils import *
import time
from subprocess import call

def make_email_msgs(fileList, configItems):
  jobStatus = True
  if len(fileList) < 1:
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


def getRowCnt(baseUrl, whereClause):
  qry = '.json?$select=count(*) '  + whereClause
  qry = baseUrl + qry
  count = None
  try:
    r = requests.get( qry)
    cnt =  r.json()
    count =  int(cnt[0]['count'])
    print "grabbing " + str(count) + ' records'
  except Exception, e:
    print str(e)
  return count

def getQryFull(qry):
  print qry
  try:
    r = requests.get( qry )
    if r.status_code != 200:
      return None
    return r.json()
  except Exception, e:
    print str(e)
    return None


def pageThroughResultsSelect(baseUrl, whereClause, qry_cols):
  returned_records = 0
  offset = 0
  all_results = []
  row_cnt = getRowCnt(baseUrl, whereClause)
  limit = 2000
  if row_cnt > 2000:
    limit = 2000
  while offset < row_cnt:
    limit_offset = "&$limit=%s&$offset=" % (limit) + str(offset)
    qry = baseUrl + '.geojson?$select='+qry_cols+ whereClause+ limit_offset
    #print qry
    try:
      results = getQryFull(qry)
      results = results['features']
    except Exception, e:
      print str(e)
      break
    try:
      all_results =  all_results + results
    except Exception, e:
      print str(e)
      break
    offset = offset + 2000
    #print offset
  resultsDict = {
    "type": "FeatureCollection",
    "features": all_results,
     "crs": {
        "type": "name",
        "properties": {
            "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
        }
      }
  }
  return resultsDict


def main():
  qry = "https://data.sfgov.org/resource/ktji-gk7t.json?$select=* where (service_name = 'Catch Basin Maintenance' OR service_name  = 'Sewer Issues')"
  config_inputdir = '/home/ubuntu/sewer-drains-311-calls/configs/'
  configFile = 'configs.yaml'
  cI =  ConfigItems(config_inputdir , configFile)
  configItems = cI.getConfigs()
  sftp_url = configItems['sftp_url']
  file_path = configItems['file_path']
  remote_put_path = configItems['remote_put_path']
  password=base64.b64decode(configItems['password'])
  lte = logETLLoad(config_inputdir, configItems)
  baseUrl = "data.sfgov.org"
  baseUrl = "https://"+ baseUrl +"/resource/ktji-gk7t"
  qryCols = " * "
  whereClause = '''Where (service_name = 'Catch Basin Maintenance' OR service_name  = 'Sewer Issues') AND
                  (status_notes not like '%Transferred%' AND status_notes not like '%transfered%'
                  AND  status_notes not like '%INVALID%' AND status_notes not like '%invalid'
                  AND status_notes not like '%DUPLICATE%' and status_notes not like '%duplicate%'
                  and status_notes not like '%Invalid%' and status_notes not like '%Duplicate%') AND
                  (status_notes not like 'Cancelled%' AND status_notes not like '%- Cancelled:%' AND
                  status_notes not like '%- cancelled:%' AND status_notes not like '%CANCEL%'
                  and status_notes not like '%cancel%' and status_notes not like '%     Cancel..%'
                  AND status_notes not like '% cancelled. %' AND status_notes not like '%cancelled'
                  AND status_notes not like '% Cancel %' and status_notes not like '% cancel %'
                  AND status_notes not like '% cancelled%' and status_notes not like '%cancel'
                  AND status_notes not like '%.cancel' and status_notes not like '%:     CANCEL%')
                '''

  all_results =  pageThroughResultsSelect(baseUrl, whereClause, qryCols)
  # Output to a file (JSON serialization)
  with open(file_path, 'w') as stream:
    json.dump(all_results, stream, indent=2)
  call(["npm", "run", "calls311ToShp"])
  with pysftp.Connection(sftp_url, username=configItems['username'], password= base64.b64decode(configItems['password']) ) as sftp:
    if os.path.isfile(configItems['file_path_zip']):
      sftp.chdir('DPW')
      print "uploading file: " + configItems['file_path_zip'] + " At " + str(datetime.date.today())
      sftp.put(configItems['file_path_zip'])
      print 'Upload done.'
      #add emailer
      msg_dict = make_email_msgs("drains_sewers_311_shapefile.zip", configItems)
      today = " for " +str(time.strftime("%m/%d/%Y"))
      msg = lte.sendJobStatusEmail(msg_dict['subject_line'] + today, msg_dict['msg'])
    else:
      msg_dict = make_email_msgs([], configItems)
      today = " for " +str(time.strftime("%m/%d/%Y"))
      msg = lte.sendJobStatusEmail(msg_dict['subject_line'] + today, msg_dict['msg'])

if __name__ == "__main__":
    main()
