from cosmos.lib.ezflow.tool import Tool
from json_utils import json_split1fastq
from os.path import join

class Split(Tool):
    inputs = ['fastq.gz']
    outputs = ['json']
    time_req = 12*60
    mem_req = 3000

    def cmd(self,i,s,p):
        return "python {Split_fastq} {p[gz_path]} {p[gz_output_dir]} $OUT.json", {
                       'Split_fastq' : join (s['pipes'],'tools_pipe/json_utils/json_to_format_split1fastq.py')}
    
class Total_json(Tool):
    inputs = ['json']
    outputs = ['json']
    time_req = 12*60
    mem_req = 3000

    def cmd(self,i,s,p):
        return "cat {inputs_json} > $OUT.json", {
            'inputs_json': ' '.join(map(str,i['json']))
        }
        
class Format_json(Tool):
    inputs = ['json']
    #outputs = ['json']
    time_req = 12*60
    mem_req = 3000

    def cmd(self,i,s,p):
        return "python {Format_json} {inputs_json} {s[out_json]} ", {
                'Format_json' : join (s['pipes'],'tools_pipe/json_utils/json_parse.py'),
                'inputs_json': ' '.join(map(str,i['json']))
            }

    
    
