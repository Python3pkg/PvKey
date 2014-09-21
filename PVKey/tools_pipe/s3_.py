from cosmos.lib.ezflow.tool import Tool
#from json_utils import json_split1fastq
from os.path import join

class S3Upload(Tool):
    inputs = ['fastq.gz']
    cpu_req=4
    time_req = 12*60
    mem_req = 3000

    def cmd(self,i,s,p):
        return "python {S3_up} -np {self.cpu_req} 's3://{p[bucket]}/{p[gz_path]}' '{p[dict]}/{p[gz_path_local]}' ", {
                       'S3_up' : join (s['pipes'],'tools_pipe/s3_utils/s3_mp_download.py')}
    