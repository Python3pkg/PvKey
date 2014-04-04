from cosmos.lib.ezflow.dag import add_, map_, reduce_, split_, reduce_split_, sequence_, apply_
from cosmos.lib.ezflow.tool import Tool
from cosmos.Workflow.models import TaskFile
from tools_pipe import pipes, picard, gatk, samtools, mutect, json_,s3_, svdetect
from wga_settings import wga_settings
import json
from cosmos.lib.ezflow.tool import INPUT
from cosmos.lib.ezflow.dag import DAG,add_,configure,add_run, map_
from os.path import join

def Pipeline_SV():
    intervals = ('interval', [20,21])
    
    align= sequence_(
            map_(pipes.Prova),
            map_(samtools.IndexBam),
    )
    
    somatic_call = sequence_(
           
           apply_(
                  sequence_(
                        #map_(mutect.createInput),
                        #reduce_(['rgid'], mutect.Somatic, tag={'vcf': 'Mutect'}),
                        #reduce_(['vcf'], gatk.CombineVariants, 'Combine Into Raw VCFs'),
                  ),
                  sequence_(
                        reduce_(['sample_name','sample_type'], picard.MERGE_SAMS),
                        split_([intervals],svdetect.PreProcessing),
                        #map_(svdetect.PreProcessing),
                        map_(svdetect.link2SV2)
                  )
         )
    )
    
    pre_processing = sequence_(
        split_(['sample_name','sample_type'],svdetect.PreProcessing),
        #map_(svdetect.link2SV)
    )
     
    return sequence_(
        align,
        somatic_call,
        #pre_processing,
    )

def Pipeline_upload():
    download_fastq = sequence_(
        map_(s3_.S3Upload)
    )
     
    return sequence_(
        download_fastq,
    )
    
def Pipeline_split():
    split_fastq = sequence_(
        map_(json_.Split),
        apply_(
            reduce_(['gz_output_dir'],json_.Total_json)
        ),
        map_(json_.Format_json)
    )
     
    return sequence_(
        split_fastq,
    )

def Pipeline():
    testing = wga_settings['test'] 
    target = wga_settings['target']
    if testing:
        intervals = ('interval', [20])
    else:
        #intervals = ('interval', [20, 21])
        intervals = ('interval', [20])
        #intervals = ('interval',range(1,23) + ['X', 'Y'])
    glm = ('glm', ['SNP', 'INDEL'])
    
    align_to_reference = sequence_(
        apply_(
            reduce_(['sample_name', 'library', 'platform', 'platform_unit', 'chunk'], pipes.AlignAndCleanALN)
        ),
    )
    
    if target:
        remove_dup = sequence_(
            reduce_(['sample_name'], picard.MERGE_SAMS)
        )
    else:
        remove_dup = sequence_(
            reduce_(['sample_name'], picard.MarkDuplicates)
        )
       
    preprocess_alignment = sequence_(                              
        map_(samtools.IndexBam),
        apply_(
            split_([intervals],gatk.RealignerTargetCreator) #if not is_capture or testing else map_(gatk.RealignerTargetCreator)    
        ),
        map_(gatk.IndelRealigner),
        map_(gatk.BQSR),
        apply_(
            reduce_(['sample_name'], gatk.BQSRGatherer),
            map_(gatk.ApplyBQSR) #TODO I add BQSRGatherer as a parent with a hack inside ApplyBQSR.cmd
        )
    )
      
    call_variants = sequence_(
        apply_(
            #reduce_(['interval'],gatk.HaplotypeCaller,tag={'vcf':'HaplotypeCaller'}),
            reduce_split_(['interval'], [glm], gatk.UnifiedGenotyper, tag={'vcf': 'UnifiedGenotyper'}),
            combine=True
        ),
        reduce_(['vcf'], gatk.CombineVariants, 'Combine Into Raw VCFs'),
        split_([glm],gatk.VQSR), 
    )

        #apply_(
                #
        #)
        #reduce_(['sample_name'], picard.MarkDuplicates)
    
    return sequence_(
            align_to_reference,
            remove_dup,
            preprocess_alignment,
            #call_variants
    )


def Pipeline_local():
    testing = wga_settings['test']
    target = wga_settings['target']

    if testing:
        intervals = ('interval', [20])
    else:
        intervals = ('interval', [20, 21])
        #intervals = ('interval',range(1,23) + ['X', 'Y'])
    glm = ('glm', ['SNP', 'INDEL'])
    print [glm][0]
    
    align_to_reference = sequence_(
        apply_(
            reduce_(['sample_name', 'library', 'platform', 'platform_unit'], pipes.AlignAndCleanALN)
        ),
    )
    
    if target:
        remove_dup = sequence_(
        )
    else:
        remove_dup = sequence_(
            reduce_(['sample_name'], picard.MarkDuplicates)
        )
        
    preprocess_alignment = sequence_( 
        map_(samtools.IndexBam),
        map_(gatk.RealignerTargetCreator), #if not is_capture or testing else map_(gatk.RealignerTargetCreator) 
        map_(gatk.IndelRealigner), 
        map_(gatk.BQSR),
        map_(gatk.ApplyBQSR_local)
    )  
     
    
    call_variants = sequence_(
        apply_(
            #reduce_(['interval'],gatk.HaplotypeCaller,tag={'vcf':'HaplotypeCaller'}),
            map_(gatk.UnifiedGenotyper_local, tag={'vcf': 'UnifiedGenotyper'})
        )
            
    )
    
    return sequence_(
            align_to_reference,
            #remove_dup,
            #preprocess_alignment,
            #call_variants
    )
    

def Pipeline_Somatic():
    testing = wga_settings['test']
    target = wga_settings['target']

    if testing:
        intervals = ('interval', [20])
    else:
        #intervals = ('interval', [20, 21])
        intervals = ('interval',range(1,23) + ['X', 'Y'])
    glm = ('glm', ['SNP', 'INDEL'])

    align_to_reference = sequence_(
        apply_(
            reduce_(['sample_name', 'library', 'platform', 'platform_unit','sample_type','chunk','rgid'], pipes.AlignAndCleanMEM)
        ),
    )

    if target:
        remove_dup = sequence_(
            reduce_(['sample_name','sample_type','rgid'], picard.MERGE_SAMS)
        )
    else:
        remove_dup = sequence_(
            reduce_(['sample_name','sample_type','rgid'], picard.MarkDuplicates)
        )

    preprocess_alignment = sequence_(
        map_(samtools.IndexBam),
        apply_(
            split_([intervals],gatk.RealignerTargetCreator) #if not is_capture or testing else map_(gatk.RealignerTargetCreator)    
        ),
        map_(gatk.IndelRealigner),
        map_(gatk.BQSR),
        apply_(
            reduce_(['sample_name','sample_type','rgid'], gatk.BQSRGatherer),
            map_(gatk.ApplyBQSR) #TODO I add BQSRGatherer as a parent with a hack inside ApplyBQSR.cmd
        )
    )

    somatic_call = sequence_(
           
           apply_(
                  sequence_(
                        map_(mutect.createInput),
                        reduce_(['rgid','interval'], mutect.Somatic, tag={'vcf': 'Mutect'}),
                        reduce_(['vcf'], gatk.CombineVariants, 'Combine Into Raw VCFs'),
                  ),
                  sequence_(
                        #reduce_(['sample_name','sample_type'], picard.MERGE_SAMS),
                        #map_(samtools.IndexBam),
                        map_(svdetect.PreProcessing),
			#split_([intervals],svdetect.PreProcessing),
                        #map_(svdetect.PreProcessing),
                        map_(svdetect.link2SV)
                  )
         )
    )

    mutect_call = sequence_(
        map_(mutect.createInput),
        reduce_(['rgid','interval'], mutect.Somatic)
        )


    call_variants = sequence_(
        apply_(
            #reduce_(['interval'],gatk.HaplotypeCaller,tag={'vcf':'HaplotypeCaller'}),
            map_(gatk.UnifiedGenotyper_local, tag={'vcf': 'UnifiedGenotyper'})
        )

    )

    return sequence_(
            align_to_reference,
            remove_dup,
            preprocess_alignment,
            somatic_call
            #call_variants
    )
