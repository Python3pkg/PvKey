import argparse
import os
from os.path import join
import BaseSpacePy
import time

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.model.QueryParameters import QueryParameters
from s3_multipart_upload import main as s3_upload

# https://developer.basespace.illumina.com/docs/content/documentation/sdk-samples/python-sdk-overview

def download_Project(project_Name, output_folder):
    
    # initialize an authentication object using the key and secret from your app
    # Fill in with your own values

    '''
    client_key                 = <my key>
    client_secret              = <my secret>
    AppSessionId               = <my appSession id>
    BaseSpaceUrl               = 'https://api.basespace.illumina.com/'
    version                    = 'v1pre3'
    accessToken                = <my acceseToken>
    '''
   
    myAPI = BaseSpaceAPI(client_key, client_secret, BaseSpaceUrl, version, AppSessionId,AccessToken=accessToken)
    # Retrieve current user
    user = myAPI.getUserById('current')
    user=str(user)
    id_name=user.split(':')
    #print id_name[0]
    
    # Retrieve all the project associated to that user
    projects=myAPI.getProjectByUser(id_name[0], queryPars=QueryParameters( {'Limit': '100'}))
    project_found=0
    for project in projects:
        project=str(project)
        nameProject_id=project.split('-')
        if str(project_Name) in str(nameProject_id):
            project_found=1
            id_project=nameProject_id[1].split('=')
            id_project=id_project[1]
            samples=myAPI.getSamplesByProject(id_project,  queryPars=QueryParameters( {'Limit': '100'}))
            print("There are "+str(len(samples))+" samples in the requested project ("+str(project_Name)+" - ID_PROJECT "+str(id_project)+")")
            
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
            
            print(time.ctime()+" START DOWNLOADING")
            for file in samples:
                file_out=file.getFiles(myAPI)
                #print file_out
                
                for fastq in file_out:
                    fastq.downloadFile(myAPI,output_folder)
                    print(time.ctime()+" FILE "+str(fastq)+" DOWNLOADED")
                    path_file=join(output_folder,str(fastq))
                    path_S3=join(str(project_Name),str(fastq))
                    s3_upload(path_file,"bmi-ngs",path_S3)
            print(time.ctime()+" DOWNLOAD COMPLETED")
    if project_found==0:
        print("Project Not Found")

def main():
    # Parser for command-line options, arguments ---> http://docs.python.org/dev/library/argparse.html#
    # create the top-level parser
    parser = argparse.ArgumentParser(description='Download from Base Space')
    parser.add_argument('-project',type=str,help='The name of the shared project',required=True)
    parser.add_argument('-folder',help='Download folder',required=True)    
    args  = parser.parse_args()
    download_Project(args.project,args.folder)

main()

