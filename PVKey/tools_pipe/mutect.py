from cosmos.lib.ezflow.tool import Tool
from cosmos.Workflow.models import TaskFile
from cosmos.session import settings as cosmos_settings
import os
import subprocess

def list2input(l):
    return " ".join([str(x) for x in l])
    #return "cat " +" && cat ".join(map(lambda x: str(x),l))
    #return "\n".join([" ".format(n) for n in l])
"""
    print "List2Input"
    stringa=""
    for ele in l:
        print ele    
        content=str(ele)
        split=content.rsplit(':')
        last=split[-1]
        split2=last.rsplit(']')
        #print "LAST ELEMENT"
        print split2[0] 
        cmd = 'cat '+split2[0]
        print cmd
        return_code = commands.getoutput(cmd)
        stringa = stringa + return_code+"\n" 
    return stringa
"""
class Mutect(Tool):
    mem_req = 3*1024
    forward_input = True
    persist=True
    @property
    def bin(self):
        return 'java -Xmx{mem_req}m -Djava.io.tmpdir={s[tmp_dir]} -jar {s[Mutect_path]}'.format(
            self=self,s=self.settings,
            mem_req=int(self.mem_req*.9)
        )
        
class Somatic(Mutect):
    name = "Run Mutect"
    inputs = ['txt']
    outputs = ['vcf']
    # time_req = 4*60
    mem_req = 3*1024

    def cmd(self,i,s,p):
        return r"""
            out=`cat {inputs}`;
            
            {self.bin}
            --analysis_type MuTect
            --reference_sequence {s[reference_fasta_path]}
            --cosmic {s[cosmic_v54_mutect]}
            --dbsnp {s[dbsnp_mutect]}
            --intervals {p[interval]}
            $out
            --vcf $OUT.vcf
        """, {'inputs': list2input(i['txt'])}  
        
class createInput(Mutect):
    name = "Create Mutect Input"
    inputs = ['bam']
    outputs = ['txt']
    mem_req = 3*1024
    
    def cmd(self,i,s,p):   
        return 'echo "--input_file:{p[sample_type]} {input}" > $OUT.txt', {'input': ' '.join([str(x) for x in i['bam']])}   

class Somatic2(Mutect):
    name = "Run Mutect"
    inputs = ['txt']
    outputs = ['txt']
    # time_req = 4*60
    mem_req = 3*1024

    def cmd(self,i,s,p):
        return r"""
            out=`cat {inputs}`;
            
            {self.bin}
            --analysis_type MuTect
            --reference_sequence {s[reference_fasta_path]}
            --cosmic {s[cosmic_v54_mutect]}
            --dbsnp {s[dbsnp_mutect]}
            --intervals {p[interval]}
            $out
            --out $OUT.txt
        """, {'inputs': list2input(i['txt'])}  
