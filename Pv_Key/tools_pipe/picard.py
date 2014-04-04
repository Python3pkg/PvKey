from cosmos.lib.ezflow.tool import Tool
from cosmos.Workflow.models import TaskFile

import os
opj = os.path.join

def list2input(l):
    print " ".join(map(lambda x: 'INPUT='+str(x),l))
    return " ".join(map(lambda x: 'INPUT='+str(x),l))

class Picard(Tool):
    time_req = 12*60
    mem_req = 3*1024
    cpu_req=1
    extra_java_args=''

    @property
    def picard_bin(self):
        return 'java{self.extra_java_args} -Xmx{mem_req}m -Djava.io.tmpdir={s[tmp_dir]} -Dsnappy.loader.verbosity=true'.format(
            self=self,
            mem_req=int(self.mem_req*.8),
            s=self.settings
            )

    @property
    def bin(self):
        return self.picard_bin+' -jar {0}'.format(
            os.path.join(self.settings['Picard_dir'],self.jar)
        )

class CollectMultipleMetrics(Picard):
    name = "Collect Multiple Metrics"
    inputs = ['bam']
    outputs = ['metrics']
    # time_req = 4*60
    mem_req = 3*1024

    jar = 'CollectMultipleMetrics.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            REFERENCE_SEQUENCE={s[reference_fasta_path]}
            INPUT={i[bam][0]}
            OUTPUT=$OUT.metrics
        """

class AddOrReplaceReadGroups(Picard):
    name = "Add or Replace ReadGroups"
    inputs = ['sam']
    outputs = ['bam']
    # time_req = 4*60
    mem_req = 3*1024

    jar = 'AddOrReplaceReadGroups.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[sam][0]}
            OUTPUT=$OUT.bam
            RGID={p[platform_unit]}
            RGLB={p[library]}
            RGSM={p[sample_name]}
            RGPL={p[platform]}
            RGPU={p[platform_unit]}
        """

class FIXMATE(Picard):
    name = "Fix Mate Information"
    inputs = ['bam']
    outputs = ['bam']
    # time_req = 4*60
    mem_req = 3*1024

    jar = 'FixMateInformation.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam][0]}
            OUTPUT=$OUT.bam
            VALIDATION_STRINGENCY=LENIENT
        """

class REVERTSAM(Picard):
    inputs = ['bam']
    outputs = ['bam']
    mem_req = 12*1024
    cpu_req=2
    #succeed_on_failure = False

    extra_java_args =' -XX:ParallelGCThreads={0}'.format(cpu_req+1)

    jar = 'RevertSam.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam][0]}
            OUTPUT=$OUT.bam
            VALIDATION_STRINGENCY=SILENT
            MAX_RECORDS_IN_RAM=4000000
        """

class SAM2FASTQ_byrg(Picard):
    inputs = ['bam']
    outputs = ['dir']
    # time_req = 180
    mem_req = 12*1024
    #succeed_on_failure = True

    jar = 'SamToFastq.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam][0]}
            OUTPUT_DIR=$OUT.dir
            OUTPUT_PER_RG=true
            VALIDATION_STRINGENCY=LENIENT
        """

class SAM2FASTQ(Picard):
    """
    Assumes sorted
    """
    inputs = ['bam']
    outputs = ['1.fastq','2.fastq']
    mem_req = 3*1024
    succeed_on_failure = True

    jar = 'SamToFastq.jar'

    def cmd(self,i,s,p):
        import re
        return r"""
            {self.bin}
            INPUT={i[bam][0]}
            FASTQ=$OUT.1.fastq
            SECOND_END_FASTQ=$OUT.2.fastq
            VALIDATION_STRINGENCY=SILENT
        """

class MERGE_SAMS(Picard):
    name = "Merge Sam Files"
    mem_req = 3*1024
    inputs = ['bam']
    outputs = ['bam']
    default_params = { 'assume_sorted': False}
    
    jar = 'MergeSamFiles.jar'
    
    
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            {inputs}
            OUTPUT=$OUT.bam
            SORT_ORDER=coordinate
            MERGE_SEQUENCE_DICTIONARIES=True
            ASSUME_SORTED={p[assume_sorted]}
        """, {
        'inputs' : "\n".join(["INPUT={0}".format(n) for n in i['bam']])
        }
                
class CLEAN_SAM(Picard):
    name = "Clean Sams"
    mem_req = 4*1024
    inputs = ['bam']
    outputs = ['bam']
        
    jar = 'CleanSam.jar'
    
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            I={i[bam][0]}
            O=$OUT.bam
            VALIDATION_STRINGENCY=SILENT
        """

class SORT_BAM(Picard):
    name = "Sort BAM"
    mem_req = 4*1024
    inputs = ['bam']
    outputs = ['bam']

    jar = 'SortSam.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            I={i[bam][0]}
            O=$OUT.bam
            SORT_ORDER=coordinate
        """

class MarkDuplicates(Picard):
    name = "Mark Duplicates"
    mem_req = 6*1024
    time_req = 20*60
    inputs = ['bam']
    outputs = [TaskFile(name='bam',basename='markdupes.bam'),
               TaskFile(name='metrics',basename='markdupes.metrics')]
    persist=True
        
    jar = 'MarkDuplicates.jar'
    
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            {inputs}
            O=$OUT.bam
            METRICS_FILE=$OUT.metrics
            ASSUME_SORTED=True
            CREATE_INDEX=True
        """, {'inputs':list2input(i['bam'])}

class INDEX_BAM(Picard):
    name = "Index Bam Files"
    mem_req = 4*1024
    forward_input = True
    inputs = ['bam']
        
    jar = 'BuildBamIndex.jar'
    
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam][0]}
            OUTPUT={i[bam][0]}.bai
        """