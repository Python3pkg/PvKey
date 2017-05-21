from cosmos.lib.ezflow.tool import Tool
from cosmos.Workflow.models import TaskFile
from cosmos.session import settings as cosmos_settings
from algorithm_settings import settings
import os
import subprocess
import re

def parse(file_log):
    input=[str(x) for x in file_log]
    file_to_parse=str(input[0])
    return file_to_parse

class PreProcessing(Tool):
    name = "SVDetect Bam Preprocessing"
    inputs = ['bam']
    outputs = ['log']
    # time_req = 4*60
    mem_req = 3*1024
    forward_input = True
    persist=True
    
    def cmd(self,i,s,p):
        return r"""
            perl {s[SVDetect_BamPreProc]}
            -t 1 
            -p 1            
            {i[bam][0]}
            >&
            $OUT.log
        """
        
class link2SV(Tool):
    name = "SVDetect Structural and Copy Number Variants"
    inputs = ['log','bam']
    outputs = ['conf','dir']
    cpu_req = 4
    mem_req = 4*1024
    def cmd(self,i,s,p):
        return r"""
            python {s[SVDetect_parse_log]} 
            -log {stringa} 
            -out $OUT.conf 
            -nt {nt} 
            {setting_sv} 
            -mates_file {mates} 
            -cmap_file {s[cmap_file_svdetect]}
            -mates_file_ref {s[bwa_reference_fasta_path]}
            -output_dir $OUT.dir
            -chr {p[interval]}
            ;
            perl {s[SVDetect]} linking 
            -conf $OUT.conf
            ;
            perl {s[SVDetect]} filtering 
            -conf $OUT.conf
            ;
            perl {s[SVDetect]} links2SV 
            -conf $OUT.conf
            ;
	    """, {
                  'stringa': parse(i['log']), 
                  'nt': self.cpu_req,
                  'mates' : parse(i['bam']),
                  'setting_sv': settings.sv_settings()}
            
class link2SV2(Tool):
    name = "SVDetect Structural and Copy Number Variants"
    inputs = ['log','bam']
    outputs = ['conf','dir']
    cpu_req = 4
    mem_req = 4*1024
    def cmd(self,i,s,p):
        return r"""
            python {s[SVDetect_parse_log]} 
            -log {stringa} 
            -out $OUT.conf 
            -nt {nt} 
            {setting_sv} 
            -mates_file {mates} 
            -cmap_file {s[cmap_file_svdetect]}
            -mates_file_ref {s[bwa_reference_fasta_path]}
            -output_dir $OUT.dir
            -chr {p[interval]}
            """, {
                  'stringa': parse(i['log']), 
                  'nt': self.cpu_req,
                  'mates' : parse(i['bam']),
                  'setting_sv': settings.sv_settings()}
