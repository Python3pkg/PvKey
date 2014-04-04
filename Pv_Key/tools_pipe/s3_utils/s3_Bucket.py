import os
from os.path import join
import boto

def getList(bucket,project,out_dict):
    json_bucket=bucket
    conn = boto.connect_s3()
    bucket = conn.get_bucket(bucket)
    num_key=0
    for key in bucket.list(project):
        num_key+=1
    file_json = os.path.join(out_dict,'input.json')
    file_out=open(file_json,'w')
    file_out.write('[\n')
    i=0
    for key in bucket.list(project):
        file_out.write('  {\n'+
                       '    \"gz_path\": '+'\"'+key.name.encode('utf-8')+'\",\n'+
                       '    \"gz_path_local\": '+'\"'+key.name.encode('utf-8').replace(" ", "_")+'\",\n'+
                       '    \"bucket\": '+'\"'+json_bucket+'\",\n'+
                       '    \"dict\": '+'\"'+str(out_dict)+'\"\n')
        if i==num_key-1:
            file_out.write('  }\n')
        else:
            file_out.write('  },\n')
        i+=1
    file_out.write(']')
    return file_json

