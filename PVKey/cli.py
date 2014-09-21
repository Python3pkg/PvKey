import argparse
from argparse import RawTextHelpFormatter
from argparse import ArgumentTypeError
import json
import sys
import glob, os
import pprint
from os.path import join
from cosmos.config import settings
from cosmos.Workflow import cli
from cosmos.lib.ezflow.tool import INPUT
from cosmos.lib.ezflow.dag import DAG,add_,configure,add_run, map_
from pipeline import Pipeline, Pipeline_local, Pipeline_Somatic, Pipeline_split, Pipeline_upload
from wga_settings import wga_settings
from cosmos import session
from tools_pipe.json_utils import json_creator
from tools_pipe.s3_utils import s3_Bucket

def json_(workflow,input_dict,**kwargs):
    """
    Input file is a json of the following format:
    [
        {
            'chunk': 001,
            'library': 'LIB-1216301779A',
            'sample_name': '1216301779A',
            'platform': 'ILLUMINA',
            'platform_unit': 'C0MR3ACXX.001'
            'pair': 0, #0 or 1
            'path': '/path/to/fastq'
        },
        {..}
    ]
    """
    input_json = json.load(open(input_dict,'r'))
    inputs = [ INPUT(name='fastq.gz',path=i['path'],fmt='fastq.gz',tags=i,stage_name='Load Input Fastqs') for i in input_json ]

    DAG(ignore_stage_name_collisions=True).sequence_(
         add_(inputs),
         Pipeline(),
         configure(wga_settings),
         add_run(workflow)
    )

def json_local(workflow,input_dict,**kwargs):
    """
    Input is a folder where each file is a json of the following format:

    [
        {
            'library': 'LIB-1216301779A',
            'sample_name': '1216301779A',
            'platform': 'ILLUMINA',
            'platform_unit': 'C0MR3ACXX.001'
            'pair':1
            'path': '/path/to/fastq'
        },
        {
            'library': 'LIB-1216301779A',
            'sample_name': '1216301779A',
            'platform': 'ILLUMINA',
            'platform_unit': 'C0MR3ACXX.001'
            'pair':2
            'path': '/path/to/fastq'..}
    ]
    """
    dirList=os.listdir(input_dict)
    for files in dirList:
        print input_dict+files
        input_json = json.load(open(input_dict+files,'r'))
        inputs = [ INPUT(name='fastq.gz',path=i['path'],fmt='fastq.gz',tags=i,stage_name='Load Input Fastqs') for i in input_json ]
        for i in inputs:
            print i
        DAG(ignore_stage_name_collisions=True).sequence_(
            add_(inputs),
            Pipeline_local(),
            configure(wga_settings),
            add_run(workflow)
        )

def json_somatic(workflow,input_dict,**kwargs):
    """
    Input file is a json of the following format:

    [
        {
	    "chunk": "001",
            "library": "LIB-1216301779A",
            "platform": "ILLUMINA",
            "platform_unit": "C0MR3ACXX.001",
	    "rgid": "BC18-06-2013",
	    "sample_name": "BC18-06-2013LyT_S5_L001",
	    "pair": "1",
	    "path": "/path/to/fastq.gz",
	    "sample_type": "normal or tumor"
        },
        {..}
    ]
    """
    
    input_json = json.load(open(input_dict,'r'))
    inputs = [ INPUT(name='fastq.gz',path=i['path'],fmt='fastq.gz',tags=i,stage_name='Load Input Fastqs') for i in input_json ]

    DAG(ignore_stage_name_collisions=True).sequence_(
         add_(inputs),
         Pipeline_Somatic(),
         configure(wga_settings),
         add_run(workflow)
    )

def fastq_(workflow,input_dict,output_dict,output_json,**kwargs):
    
    json_fastq_to_split=json_creator.json_out(input_dict,output_dict)
    input_json = json.load(open(json_fastq_to_split,'r'))
    inputs = [ INPUT(name='fastq.gz',path=i['gz_path'],fmt='fastq.gz',tags=i,stage_name='Load Input Fastqs') for i in input_json ]
        
    DAG(ignore_stage_name_collisions=True).sequence_(
         add_(inputs),
         Pipeline_split(),
         configure(wga_settings),
         add_run(workflow)
    )
    
def upload_(workflow,bucket,project,out_dict,**kwargs):        
    project_folder=join(out_dict,project.replace(" ", "_"))
    if not os.path.exists(project_folder):
            os.makedirs(project_folder)
    json_fastq_to_upload=s3_Bucket.getList(bucket,project,out_dict)
    input_json = json.load(open(json_fastq_to_upload,'r'))
    inputs = [ INPUT(name='fastq.gz',path=i['gz_path'],fmt='fastq.gz',tags=i,stage_name='Load Input Fastqs') for i in input_json ]
        
    DAG(ignore_stage_name_collisions=True).sequence_(
         add_(inputs),
         Pipeline_upload(),
         configure(wga_settings),
         add_run(workflow)
    )

def main():
    # Parser for command-line options, arguments ---> http://docs.python.org/dev/library/argparse.html#
    # create the top-level parser
    parser = argparse.ArgumentParser(description='WGA')
    parser.add_argument('-test',action='store_true',default=False,help='Signifies this as a test run')
    parser.add_argument('-lustre',action='store_true',default=False,help='submits to erik\'s special orchestra cluster')
    parser.add_argument('-tmp','--temp_directory',type=str,default=settings['working_directory'],help='Specify a wga_settings[tmp_dir]. Defaults to working_directory specified in cosmos.ini')
    # Parser for command-line options, sub-commands ---> http://docs.python.org/dev/library/argparse.html#sub-commands
    subparsers = parser.add_subparsers(title="Commands", metavar="<command>")
    # create the parser for the "json" command
    json_sp = subparsers.add_parser('json',help="Parallel running. Input is FASTQs, encoded as a json file",description=json_.__doc__,formatter_class=RawTextHelpFormatter)
    json_sp.set_defaults(func=json_)
    cli.add_workflow_args(json_sp)
    json_sp.add_argument('-il','--input_dict',type=str,help='Inputs, see script comments for format.',required=True)
    json_sp.add_argument('-ped','--pedigree_file',type=file,help='A Pedigree File to pass to all GATK tools')
    json_sp.add_argument('-capture','--capture',action="store_true",default=False,help='Signifies that a capture technology was used.  Currently'
                                                                                                        'all this does is remove -an DP to VQSR')
    json_sp.add_argument('-target',default='False',choices=['True','False'],help='If -target is True the MarkDuplicates step will not be performed')


    json_lo = subparsers.add_parser('json_local',help="Local running (no parallelization). Input is FASTQs, encoded as a json file",description=json_local.__doc__,formatter_class=RawTextHelpFormatter)
    json_lo.set_defaults(func=json_local)
    cli.add_workflow_args(json_lo)
    json_lo.add_argument('-il','--input_dict',type=str,help='Inputs, see script comments for format.',required=True)
    json_lo.add_argument('-target',default='False',choices=['True','False'],help='If -target is True the MarkDuplicates step will not be performed')
    
    
    json_so = subparsers.add_parser('json_somatic',help="Parallel running of tumor-normal sample. Input is FASTQs, encoded as a json file",description=json_local.__doc__,formatter_class=RawTextHelpFormatter)
    json_so.set_defaults(func=json_somatic)
    cli.add_workflow_args(json_so)
    json_so.add_argument('-il','--input_dict',type=str,help='Inputs, see script comments for format.',required=True)
    json_so.add_argument('-target',default='False',choices=['True','False'],help='If -target is True the MarkDuplicates step will not be performed')
     
    
    fastq_chunk = subparsers.add_parser('fastq_chunk',help="Split FASTQs.gz in chunk")
    fastq_chunk.set_defaults(func=fastq_)
    cli.add_workflow_args(fastq_chunk)
    fastq_chunk.add_argument('-il','--input_dict',type=str,help='Inputs directory of gz files',required=True)
    fastq_chunk.add_argument('-out','--output_dict',type=str,help='Output directory of gz chunk',required=True)
    fastq_chunk.add_argument('-out_json','--output_json',type=str,help='Output json',required=True)
    
    fastq_chunk.add_argument('-target',default='False',choices=['True','False'],help='If -target is True the MarkDuplicates step will not be performed')

    upload_s3 = subparsers.add_parser('upload_s3',help="Upload from s3 bucket to ephemeral drive")
    upload_s3.set_defaults(func=upload_)
    cli.add_workflow_args(upload_s3)
    upload_s3.add_argument('-b','--bucket',type=str,help='Bucket_s3',required=True)
    upload_s3.add_argument('-p','--project',type=str,help='Project folder where are stored the fastq.gz',required=True)
    upload_s3.add_argument('-out_dict','--out_dict',type=str,help='Directory di output',required=True)

    wf,kwargs = cli.parse_args(parser)
    wga_settings['test'] = kwargs['test']
    wga_settings['lustre'] = kwargs['lustre']
    wga_settings['tmp_dir'] = kwargs.get('temp_directory')
    wga_settings['capture'] = kwargs.get('capture',None)
    ped_file = kwargs.get('pedigree',None)
    wga_settings['pedigree'] = ped_file.name if ped_file else None
    target = kwargs.get('target')
    wga_settings['target'] = True if target == 'True' else False
    output_dir_fastq = kwargs.get('output_dict')
    wga_settings['output_dir_gz'] = output_dir_fastq if output_dir_fastq else ""
    output_json_fastq = kwargs.get('output_json')
    wga_settings['out_json'] = output_json_fastq if output_json_fastq else ""
    
    wf.log.info('wga_settings =\n{0}'.format(pprint.pformat(wga_settings,indent=2)))
    kwargs['func'](wf,**kwargs)
    
session.get_drmaa_native_specification = wga_settings['get_drmaa_native_specification']
main()

