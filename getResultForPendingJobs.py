
# coding: utf-8

# In[1]:

import pymongo
from pymongo import MongoClient
import pyxnat as py
import requests
import json

session = "https://msmetrix.icometrix.com/users-sessions/api/v1/sessions"
jobs = "https://msmetrix.icometrix.com/msmetrix-jobs/api/v1/projects/9DC_9E7/jobs";

def getPendingJobs():    
    client = MongoClient('146.169.32.150', 27017)    
    db = client['icometrix']
    icometrix_jobs = db.jobs
    newJobs = icometrix_jobs.find({"Job_Status":"Pending"})
    return newJobs

def logoutFromIcometrix(session_response):
    delete_response = requests.delete(session, cookies=session_response.cookies)
    print delete_response.headers
    
def logIntoIcometrix():
    user = "proteinsAreDifficult"
    pw = "130915_Pc"
    project = "9DC_9E7"
    session_response = requests.get(session, auth=(user, pw))
    return session_response
    
def updateStatus(newStatus, guidChangeJob):
    client = MongoClient('146.169.32.150', 27017)    
    db = client['icometrix']
    icometrix_jobs = db.jobs
    #print icometrix_jobs.count()
    criteria = {"USUBJID": guidChangeJob['USUBJID'], "Session": guidChangeJob['Session'], "Job_GUID": guidChangeJob['Job_GUID']}
    newValue = {"Job_Status": newStatus};
    result = icometrix_jobs.update_one(criteria, {"$set":newValue})
    #print result.matched_count
    #print result.modified_count
    
def checkProgressOfPendingJob(pendingJob, session_response):
    guid = pendingJob['Job_GUID']
    usubjid = pendingJob['USUBJID'] 
    pending_job_url = jobs + "/"+guid
    #print pending_job_url
    job_data = requests.get(pending_job_url, cookies=session_response.cookies).json()
    #print job_data['status']
    return job_data['status'] 

def formatForMorphology(finished_job, results):
    USUBJID = finished_job['USUBJID'];
    MODTC = finished_job['PRSTDTC'];
    MO = {};
    MO['USUBJID'] = USUBJID
    MO['STUDYID'] = "OPTIMISE"
    MO['DOMAIN'] = "MO"
    MO['MOSEQ'] = finished_job['Job_GUID']
    MO['MOLNKID'] = ""
    MO['MOREFID'] = ""
    MO['MOTESTCD'] = ""
    MO['MOTEST'] = "Lesion volume"
    MO['MOORRES'] = results
    MO['MOORRESU'] = "ml"
    MO['MOSTRESC'] = ""
    MO['MOSTRESN'] = ""
    MO['MOSTRESU'] = ""
    MO['MOLOC'] = "Brain"
    MO['MOSLOC'] = ""
    MO['MOLAT"'] = ""
    MO['MOMETHOD'] = "Icometrix"
    MO['MOANMETH'] = ""
    MO['MODTC'] = MODTC
    return json.loads(json.dumps(MO)); 

def saveResultsToOptimise(finished_job, mo_json):
    client = MongoClient('146.169.32.150', 27017)    
    optimise_db = client['Optimise']
    optimise_collection = optimise_db.dataStream
    post_id = optimise_collection.insert_one(mo_json).inserted_id
    print post_id
    
def saveResultsToIcometrix(finished_job, results, report):
    client = MongoClient('146.169.32.150', 27017)    
    icometrix_db = client['icometrix']
    icometrix_jobs = icometrix_db.jobs
    criteria = {"USUBJID": finished_job['USUBJID'], "Session": finished_job['Session'], "Job_GUID": finished_job['Job_GUID']}
    newValue = {"Results": results, "Report": report};
    result = icometrix_jobs.update_one(criteria, {"$set":newValue})    
    
def saveResults(finished_job, results, report, mo_json):    
    saveResultsToIcometrix(finished_job, results, report)
    saveResultsToOptimise(finished_job, mo_json)
    
def getResultsOfSuccessfulJob(finished_job, session_response):
    guid = finished_job['Job_GUID'] 
    result_url = "https://msmetrix.icometrix.com/msmetrix-jobs/api/v1/projects/9DC_9E7/jobs/"+guid
    job_data = requests.get(result_url, cookies=session_response.cookies).json()
    return job_data;

def getLesionVolumeFromCrossSectionalPipeline(job_data):
    summaryResults = {"Results": [], "LesionVolume":[], "Report": ""}
    for item in job_data['results_data']:
        if (item['type']=='crosssectional'):
            summaryResults['Results'].append(item['data']['results_dict']);
            summaryResults['LesionVolume'].append(item['data']['results_dict']['lesions']);
        if (item['type']=='qc-result'):
            files_data = item['files_data'];
            for file in files_data:
                if (file['format']=='pdf'):
                    summaryResults['Report'] = file['uris'][0];
    #print results;
    return summaryResults;

def getLesionVolumeFromLongitudinalPipeline(job_data):
    summaryResults = {"Results": [], "LesionVolume":[], "Report": ""}
    for item in job_data['results_data']:
        if (item['type']=='longitudinal'):
            oldLesionVolume = item['data']['results_dict']['volumes_t1']['lesions']
            newLesionVolume = item['data']['results_dict']['lesions_volumes']['new_lesions_volume']; 
            summaryResults['LesionVolume'].append(oldLesionVolume+newLesionVolume);
            summaryResults['Results'].append(item['data']['results_dict']);
        if (item['type']=='qc-result'):
            files_data = item['files_data'];
            for file in files_data:
                if (file['format']=='pdf'):
                    summaryResults['Report'] = file['uris'][0];
    return summaryResults;

def getPipeline(job_data):
    for item in job_data['results_data']:
        if (item['type']=='longitudinal'):
            print 'longitudinal' 
            return 'longitudinal'
    print 'crossSectional'
    return 'crossSectional';
        
    
    


# In[2]:

pendingJobs = getPendingJobs()
print pendingJobs.count()
if (pendingJobs.count() > 0):
    session_response = logIntoIcometrix()
    for pendingJob in pendingJobs:
        progress = checkProgressOfPendingJob(pendingJob, session_response)
        if (progress == 'success'):
            job_data = getResultsOfSuccessfulJob(pendingJob, session_response) 
            if (getPipeline(job_data)=='longitudinal'):
                summaryResults = getLesionVolumeFromLongitudinalPipeline(job_data)
            else:    
                summaryResults = getLesionVolumeFromCrossSectionalPipeline(job_data)
            
            if ((len(summaryResults['Results']) > 0) and (len(summaryResults['LesionVolume']))):
                json_mo = formatForMorphology(pendingJob, summaryResults['LesionVolume'][0]);
                saveResults(pendingJob, summaryResults['Results'], summaryResults['Report'], json_mo);
                updateStatus(progress, pendingJob)
   
    logoutFromIcometrix(session_response);


# In[ ]:

#             print results
#             print report
            
            
#             for item in job_data['results_data']:
#                 if (item['type']=='crosssectional'):
#                     results = item['data']['results_dict'];
#                 if (item['type']=='qc-result'):
#                     files_data = item['files_data'];
#                     for file in files_data:
#                         if (file['format']=='pdf'):
#                             report = file['uris'][0];
            #json_mo = formatForMorphology(pendingJob, results);
            #saveResults(pendingJob, results, report, json_mo);
            #updateStatus(progress, pendingJob)
            #print results
            #print report

