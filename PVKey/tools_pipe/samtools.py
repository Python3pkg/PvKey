from cosmos.lib.ezflow.tool import Tool

class FilterBamByRG(Tool):
    # name = "Filter Bam By RG"
    inputs = ['bam']
    outputs = ['bam']
    time_req = 12*60
    mem_req = 3000

    def cmd(self,i,s,p):
        return "{s[samtools_path]} view -h -b -r {p[rgid]} {i[bam][0]} -o $OUT.bam"
    
class IndexBam(Tool):
    inputs = ['bam']
    outputs = ['bai','bam']
    time_req = 12*60
    mem_req = 3000

    def cmd(self,i,s,p):
        return "{s[samtools_path]} index {i[bam][0]} $OUT.bai && cp {i[bam][0]} $OUT.bam"
