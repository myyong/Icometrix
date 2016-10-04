
# coding: utf-8

# In[14]:

import pymongo
from pymongo import MongoClient
import pyxnat as py
import os
import os.path
import zipfile
import requests

class cd:
    """Context manager for changing the current working directory"""
    def __init__(self, newPath):
        self.newPath = os.path.expanduser(newPath)

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)

session = "https://msmetrix.icometrix.com/users-sessions/api/v1/sessions"
jobs = "https://msmetrix.icometrix.com/msmetrix-jobs/api/v1/projects/9DC_9E7/jobs";

def getNewJobs():    
    client = MongoClient('146.169.32.150', 27017)    
    db = client['icometrix']
    icometrix_jobs = db.jobs
    print icometrix_jobs.count()
    newJobs = icometrix_jobs.find({"Job_Status":"New"})
    print newJobs.count()
    return newJobs

def prepDestinationForScans(usubjid, session_id):
    directory = "/Users/myyong/Documents/Images/tmp"+"/"+usubjid+"/"+session_id
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

def logIntoImageRepository():
    cif = py.Interface(server="http://cif-xnat.hh.med.ic.ac.uk", 
                    user='myyong',
                    password='150914_Bg',
                    cachedir='/Users/myyong/Documents/Images/tmp/')
    return cif

def prepScansForNewJob(newJob):
    cif = logIntoImageRepository()
    project_id = newJob['Project']
    subject_id = newJob['USUBJID']
    session_id = newJob['Session']  
    directory = prepDestinationForScans(subject_id, session_id) 
    allscans = cif.select.project(project_id).subject(subject_id).experiment(session_id).scans().get()
    url = "/projects/"+project_id+"/subjects/"+subject_id+"/experiments/"+session_id+"/scans/"
    for scan in allscans: 
        scan_url = url+scan+"/resources/DICOM"
        ascan = cif.select(scan_url)
        ascan.get(directory, True)
    return directory

def zipdir(path, ziph):
    for root, dirs, filenames in os.walk(path):
        for file in filenames:
            if file.endswith('.dcm'):
                ziph.write(file)
                  
def zipScans(directory):
    zippedFName = directory+'/4icometrix.zip'
    zipf = zipfile.ZipFile(zippedFName, 'w', zipfile.ZIP_DEFLATED)
    with cd(directory):
        zipdir(directory, zipf)
    zipf.close()
    return zippedFName

def logIntoIcometrix():
    user = "proteinsAreDifficult"
    pw = "130915_Pc"
    project = "9DC_9E7"
    session_response = requests.get(session, auth=(user, pw))
    return session_response

def confirmFileExists(fileName):
    print fileName
    if os.path.isfile(fileName):
        print "file found"
        return True
    else:
        return False;

def readyToSetJobs(session_response, fileName):
    loggedIn = False;
    fileExists = False;
    if ('Set-Cookie' in session_response.headers):
        loggedIn = True    
    if ((loggedIn)and(confirmFileExists(fileName))):
        return True
    else:
        return False;

def logoutFromIcometrix(session_response):
    delete_response = requests.delete(session, cookies=session_response.cookies)
    print delete_response.headers
        
def sendScans(fileName):
    print fileName
    session_response = logIntoIcometrix()
    print session_response.headers
    if (readyToSetJobs(session_response, fileName)): 
        fileobj = open(fileName, 'rb')
        submit_response = requests.post(jobs, cookies=session_response.cookies, files={"file": ("4icometrix.zip", fileobj)}) 
        job_info = submit_response.json()
        print job_info
        logoutFromIcometrix(session_response)
        return job_info['guid'] 
    
def updateGUID(newGUID, guidChangeJob):
    client = MongoClient('146.169.32.150', 27017)    
    db = client['icometrix']
    icometrix_jobs = db.jobs
    criteria = {"USUBJID": guidChangeJob['USUBJID'], "Session": guidChangeJob['Session']}
    newValue = {"Job_GUID": newGUID, "Job_Status": "Pending"};
    result = icometrix_jobs.update_one(criteria, {"$set":newValue})
        


# In[ ]:

newJobs = getNewJobs()
for newJob in newJobs:
    directory = prepScansForNewJob(newJob)
    #print directory+'/DICOM' 
    zippedFName = zipScans(directory+'/DICOM')
    guid = sendScans(zippedFName)
    print guid
    updateGUID(guid, newJob)  


# In[12]:




# In[12]:




# In[ ]:



