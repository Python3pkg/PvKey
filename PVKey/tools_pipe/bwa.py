from cosmos.lib.ezflow.tool import Tool

class MEM(Tool):
    name = "BWA MEM Paired End Mapping"
    mem_req = 10*1024
    cpu_req = 1
    time_req = 120
    inputs = ['fastq.gz']
    outputs = ['sam']

    def cmd(self,i,s,p):
        """
        Expects tags: chunk, library, sample_name, platform, platform_unit, pair
        """
        return r"""
            {s[bwa_path]} mem
            -M
            -R "@RG\tID:{p[platform_unit]}\tLB:{p[library]}\tSM:{p[sample_name]}\tPL:{p[platform]}\tPU:{p[platform_unit]}"
            {s[bwa_reference_fasta_path]}
            {i[fastq.gz][0]}
            {i[fastq.gz][1]}
            > $OUT.sam
            """
class ALN(Tool):
    name = "BWA ALN Paired End Mapping"
    mem_req = 1*1024
    cpu_req = 1
    time_req = 120
    inputs = ['fastq.gz']
    outputs = ['sam']

    def cmd(self,i,s,p):
        """
        Expects tags: chunk, library, sample_name, platform, platform_unit, pair
        """
        return r"""
            {s[bwa_path]} mem
            -M
            -R "@RG\tID:{p[platform_unit]}\tLB:{p[library]}\tSM:{p[sample_name]}\tPL:{p[platform]}\tPU:{p[platform_unit]}"
            {s[bwa_reference_fasta_path]}
            {i[fastq.gz][0]}
            {i[fastq.gz][1]}
            > $OUT.sam
            """