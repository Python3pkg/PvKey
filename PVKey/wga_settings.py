############################################
# wga_settings.py
############################################
import os
import sys

from cosmos        import session
from cosmos.config import settings

opj = os.path.join

if settings['server_name'] == 'orchestra':
    WGA_path = '/scratch/WGA/WGA'
else:
    #WGA_path = '/home/ettore/Scrivania/Harvard_Pipe/WGA'    # assuming AWS SCE
    WGA_path = '/gluster/gv0/WGA'

resource = opj(WGA_path, 'bundle/current')   # 2.3/b37
tools    = opj(WGA_path, 'tools')
#pipes      = '/home/ettore/Eclipse/NGS_Projects/Pipeline/PV_Key'
pipes  = '/gluster/gv0/WGA/PV_Key_Somatic'

wga_settings = {
    'tmp_dir'                         : settings['working_directory'],

    'GATK_source_path'                : opj(tools, 'gatk'),

    'annovarext_path'                 : opj(tools,    'annovarext'),       
    'bamUtil_path'                    : opj(tools,    'bamUtil'),              
    'bqsr_gatherer_path'              : opj(tools,    'BQSRGathererMain'),
    'bwa_path'                        : opj(tools,    'bwa'),              
    'fastqc_path'                     : opj(tools,    'fastqc'),           
    'fastqstats_path'                 : opj(tools,    'fastq-stats'),      
    'GATK_path'                       : opj(tools,    'gatk.jar'),
    'Picard_dir'                      : opj(tools,    'picard'),  
    'queue_path'                      : opj(tools,    'queue.jar'), # /Queue-2.4.9-g532efad/Queue.jar, necessary for BQSRGatherer.java
    'samtools_path'                   : opj(tools,    'samtools-0.1.18/samtools'), 
    'Mutect_path'                     : opj(tools,    'muTect-1.1.4-bin/muTect-1.1.4.jar'), 
    'SVDetect'                        : opj(tools,    'svdetect/SVDetect'), 
    'SVDetect_BamPreProc'             : opj(tools,    'svdetect/BAM_preprocessingPairs.pl'),  
    'SVDetect_parse_log'              : opj(tools,    'svdetect/parseLog.py'),
    'java7'                           : opj(tools,    'java7'),      
    'Split_fastq'                     : opj(tools,    'json_split1fastq.py'),        
    'pipes'                             : pipes,

    'resource_bundle_path'            : resource,
    #'reference_fasta_path'            : opj(resource, 'chr20_NoChr/chr20.fa'),
    #'bwa_reference_fasta_path'        : opj(resource, 'chr20_NoChr/chr20.fa'),
    'reference_fasta_path'            : opj(resource, 'human_g1k_v37.fasta'),
    'bwa_reference_fasta_path'        : opj(resource, 'human_g1k_v37.fasta'),


    'dbsnp_path'                      : opj(resource, 'dbsnp_137.b37.vcf'),
    'hapmap_path'                     : opj(resource, 'hapmap_3.3.b37.vcf'),
    'omni_path'                       : opj(resource, '1000G_omni2.5.b37.vcf'),
    '1000G_phase1_highconfidence_path': opj(resource, '1000G_phase1.snps.high_confidence.vcf'),
    'mills_path'                      : opj(resource, 'Mills_and_1000G_gold_standard.indels.b37.vcf'),
    'indels_1000g_phase1_path'        : opj(resource, '1000G_phase1.indels.b37.vcf'),
    'cosmic_v54_mutect'               : opj(resource, 'b37_cosmic_v54_120711.vcf'),
    #'dbsnp_mutect'                    : opj(resource, 'dbsnp_132_b37.leftAligned.vcf'),
    
    'dbsnp_mutect'                    : opj(resource, 'dbsnp_137.b37.vcf'),
    #'cmap_file_svdetect'              : opj(resource, 'hs19_chr20.len'), 
    'cmap_file_svdetect'              : opj(resource, 'hg19.len'), 


    'genomekey_library_path'          : os.path.dirname(os.path.realpath(__file__)),

    #'get_drmaa_native_specification'  : session.default_get_drmaa_native_specification
}


os.environ['ANNOVAREXT_DATA'] = opj(WGA_path,'annovarext_data')
av_path = opj(tools,'AnnovarExtensions/')
sys.path.append(av_path)
os.environ['PYTHONPATH'] = av_path+':'+os.environ.get('PYTHONPATH','')



def get_drmaa_native_specification(jobAttempt):
    task = jobAttempt.task
    DRM  = settings['DRM']

    # did other attempts fail?
    # is_reattempt = task.jobAttempts.count() >1
    
    cpu_req  = task.cpu_requirement
    mem_req  = task.memory_requirement
    time_req = task.time_requirement
    queue    = task.workflow.default_queue

    # orchestra-specific option
    if settings['server_name'] == 'orchestra':
        if wga_settings['test'] == True:
            if jobAttempt.task.stage.name != 'Unified_Genotyper':
                time_req=10
                cpu_req=1
            
        if   time_req <= 10:        queue = 'mini'
        elif time_req <= 12 * 60:   queue = 'short'
        else:                       queue = 'long'

                            
    if DRM == 'LSF':
        s = '-R "rusage[mem={0}] span[hosts=1]" -n {1}'.format(mem_req/cpu_req, cpu_req)
#       s = '-R "rusage[mem={0}] span[hosts=1]" -n {1} -J {2}'.format(mem_req, cpu_req, jobAttempt.task.workflow.name.replace(' ','_'))

        if time_req:  s += ' -W 0:{0}'.format(time_req)
        if queue:     s += '   -q {0}'.format(queue)
        return s
    elif DRM == 'GE':
        return '-l spock_mem={mem_req}M,num_proc={cpu_req}'.format(mem_req=mem_req, cpu_req=cpu_req)
    else:
        raise Exception('DRM not supported')

    
wga_settings['get_drmaa_native_specification'] = get_drmaa_native_specification
