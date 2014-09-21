from subprocess import call
import os
from os.path import join
import argparse
from wga_settings import wga_settings

def main():
    parser = argparse.ArgumentParser(description='Combine UploadS3 - Fastq Split and Gatk Best Practice')
    parser.add_argument('-name',type=str,help='Name of the workflow',required=True)
    parser.add_argument('-b','--bucket',type=str,help='Bucket_s3',required=True)
    parser.add_argument('-p','--project',type=str,help='Project folder where are stored the fastq.gz',required=True)
    parser.add_argument('-fq_dict','--fastq.gz_dict',type=str,help='Downloaded fastq.gz Directory ',required=True)
    parser.add_argument('-out_json','--output_json',type=str,help='Output json',required=True)  
    
    args = parser.parse_args()
    args= vars(args)
    
    project_folder=join(args["fastq.gz_dict"],args["project"].replace (" ", "_"))
    if not os.path.exists(project_folder):
            os.makedirs(project_folder)
    chunk_folder=join(args["fastq.gz_dict"],args["project"].replace (" ", "_"),"Chunk")
    if not os.path.exists(chunk_folder):
            os.makedirs(chunk_folder)
    
    file_to_run=join(wga_settings['pipes'],'cli.py')
    cmds3="python "+file_to_run+" upload_s3 -n "+"\""+args["name"]+" - UploadS3"+"\""+" -b \""+args["bucket"]+"\""+" -p \""+args["project"]+"\""+" -out_dict "+args["fastq.gz_dict"]
    print cmds3 
    os.system(cmds3)
    cmd_fq="python "+file_to_run+" fastq_chunk -n "+"\""+args["name"]+" - Chunk Fastq.gz"+"\""+" -il "+"\""+project_folder+"\""+" -out "+"\""+chunk_folder+"\""+" -out_json "+args["output_json"]
    print cmd_fq 
    os.system(cmd_fq)
    cmd2="python "+file_to_run+" json -n "+"\""+args["name"]+" - GATK best practice"+"\""+" -il "+args["output_json"]
    print cmd2 
    os.system(cmd2)

main()