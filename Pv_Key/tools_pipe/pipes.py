from cosmos.lib.ezflow.tool import Tool
from cosmos.Workflow.models import TaskFile
import os
opj = os.path.join
from . import picard
from algorithm_settings import settings

class AlignAndCleanMEM(picard.Picard):
    name = "BWA Alignment and Cleaning"
    mem_req = 6*1024
    #mem_req=1*1024
    cpu_req = 4
    #cpu_req=1
    time_req = 12*60
    inputs = ['fastq.gz']
    outputs = ['bam']

    def cmd(self,i,s,p):
        """
        Expects tags: chunk, library, sample_name, platform, platform_unit, pair
        """
        return r"""
            {s[bwa_path]} mem
            -M
            -t {self.cpu_req}
            -R "@RG\tID:{p[platform_unit]}\tLB:{p[library]}\tSM:{p[sample_name]}\tPL:{p[platform]}\tPU:{p[platform_unit]}"
            {s[bwa_reference_fasta_path]}
            {i[fastq.gz][0]}
            {i[fastq.gz][1]}
            |
            {self.picard_bin} -jar {AddOrReplaceReadGroups}
            INPUT=/dev/stdin
            OUTPUT=/dev/stdout
            RGID={p[platform_unit]}
            RGLB={p[library]}
            RGSM={p[sample_name]}
            RGPL={p[platform]}
            RGPU={p[platform_unit]}
            COMPRESSION_LEVEL=0
            |
            {self.picard_bin} -jar {CleanSam}
            I=/dev/stdin
            O=/dev/stdout
            VALIDATION_STRINGENCY=SILENT
            COMPRESSION_LEVEL=0
            |
            {self.picard_bin} -jar {SortSam}
            I=/dev/stdin
            O=$OUT.bam
            SORT_ORDER=coordinate
            CREATE_INDEX=True
            """, dict (
            AddOrReplaceReadGroups=opj(s['Picard_dir'],'AddOrReplaceReadGroups.jar'),
            CleanSam=opj(s['Picard_dir'],'CleanSam.jar'),
            SortSam=opj(s['Picard_dir'],'SortSam.jar')
            )


class AlignAndCleanALN(picard.Picard):
    name = "BWA ALN Alignment and Cleaning"
    mem_req = 2*1024
    cpu_req = 4
    time_req = 12*60
    inputs = ['fastq.gz']
    outputs = ['bam']

    def cmd(self,i,s,p):
        """
        Expects tags: chunk, library, sample_name, platform, platform_unit, pair
        """
        return r"""
            {s[bwa_path]} aln {settings_bwa}
            {s[bwa_reference_fasta_path]}
            {i[fastq.gz][0]} > {i[fastq.gz][0]}.1.sai
            &&
            {s[bwa_path]} aln {settings_bwa}
            {s[bwa_reference_fasta_path]}
            {i[fastq.gz][1]} > {i[fastq.gz][1]}.2.sai
            &&
            {s[bwa_path]} sampe
            {s[bwa_reference_fasta_path]}
            {i[fastq.gz][0]}.1.sai
            {i[fastq.gz][1]}.2.sai
            {i[fastq.gz][0]}
            {i[fastq.gz][1]}
            |
            {self.picard_bin} -jar {AddOrReplaceReadGroups}
            INPUT=/dev/stdin
            OUTPUT=/dev/stdout
            RGID={p[platform_unit]}
            RGLB={p[library]}
            RGSM={p[sample_name]}
            RGPL={p[platform]}
            RGPU={p[platform_unit]}
            COMPRESSION_LEVEL=0
            |
            {self.picard_bin} -jar {CleanSam}
            I=/dev/stdin
            O=/dev/stdout
            VALIDATION_STRINGENCY=SILENT
            COMPRESSION_LEVEL=0
            |
            {self.picard_bin} -jar {SortSam}
            I=/dev/stdin
            O=$OUT.bam
            SORT_ORDER=coordinate
            CREATE_INDEX=True
            &&
            rm {i[fastq.gz][0]}.1.sai {i[fastq.gz][1]}.2.sai
            """, dict (settings_bwa=settings.bwa_aln_settings(),
            AddOrReplaceReadGroups=opj(s['Picard_dir'],'AddOrReplaceReadGroups.jar'),
            CleanSam=opj(s['Picard_dir'],'CleanSam.jar'),
            SortSam=opj(s['Picard_dir'],'SortSam.jar')
            )  
            
class Prova(picard.Picard):
    name = "BWA ALN Alignment and Cleaning"
    mem_req = 2*1024
    cpu_req = 4
    time_req = 12*60
    inputs = ['bam']
    outputs = ['bam']  
    def cmd(self,i,s,p):
        return r"""
                cp {i[bam][0]} $OUT.bam
                """
