from cosmos.lib.ezflow.tool import Tool
#from json_utils import json_split1fastq
from os.path import join

class S3Download(Tool):
    inputs = ['fastq.gz']
    outputs = ['json']
    time_req = 12*60
    mem_req = 3000

    def cmd(self,i,s,p):
        return "python {S3_down} {p[gz_path]} bmi-ngs /Output_Cluster/file_prova", {
                       'S3_down' : join (s['pipes'],'tools_pipe/s3_utils/s3_multipart_upload.py')}
    
        
class S3Upload(Tool):
    inputs = ['fastq.gz']
    outputs = ['json']
    time_req = 12*60
    mem_req = 3000

    def cmd(self,i,s,p):
        return "python {S3_down} {p[gz_path]} bmi-ngs /Output_Cluster/file_prova", {
                       'S3_down' : join (s['pipes'],'tools_pipe/s3_utils/s3_multipart_upload.py')}
    
    
